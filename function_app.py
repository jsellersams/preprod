import json
import logging
import os
import time
from typing import Any, Dict, Iterable, List, Tuple
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import requests
import azure.functions as func
import certifi
import pytds

import datetime

app = func.FunctionApp()


def _get_env(name: str, default: str = "") -> str:
    value = os.getenv(name, default)
    
    return value.strip() if isinstance(value, str) else default


def _get_int_env(name: str, default: int) -> int:
    raw = _get_env(name, str(default))
    try:
        return int(raw)
    except ValueError:
        logging.warning("Invalid integer for %s: %s. Using default=%s", name, raw, default)
        return default


def _get_bool_env(name: str, default: bool) -> bool:
    raw = _get_env(name, str(default)).lower()
    return raw in {"1", "true", "yes", "y", "on"}


def _chunked(rows: List[Dict[str, Any]], batch_size: int) -> Iterable[List[Dict[str, Any]]]:
    for i in range(0, len(rows), batch_size):
        yield rows[i : i + batch_size]


def _parse_connection_string(connection_string: str) -> Dict[str, str]:
    parsed: Dict[str, str] = {}
    for piece in connection_string.split(";"):
        if not piece or "=" not in piece:
            continue
        key, value = piece.split("=", 1)
        parsed[key.strip().lower()] = value.strip()
    return parsed


def _parse_server_and_port(server_value: str) -> Tuple[str, int]:
    cleaned = server_value.strip()
    if cleaned.lower().startswith("tcp:"):
        cleaned = cleaned[4:]

    if "," in cleaned:
        host, port_text = cleaned.rsplit(",", 1)
        try:
            return host.strip(), int(port_text.strip())
        except ValueError:
            logging.warning("Invalid SQL server port '%s', defaulting to 1433", port_text)
            return host.strip(), 1433

    return cleaned, 1433


def _fetch_records_from_sql(
    connection_string: str,
    timeout_sec: int,
) -> List[Dict[str, Any]]:
    settings = _parse_connection_string(connection_string)
    server_value = settings.get("server") or settings.get("data source")
    database = settings.get("database") or settings.get("initial catalog")

    if not server_value:
        raise ValueError("SQL connection string must include Server")
    if not database:
        raise ValueError("SQL connection string must include Database")

    host, port = _parse_server_and_port(server_value)

    def _access_token_callable() -> str:
        identity_endpoint = _get_env("IDENTITY_ENDPOINT")
        identity_header = _get_env("IDENTITY_HEADER")
        msi_endpoint = _get_env("MSI_ENDPOINT")
        msi_secret = _get_env("MSI_SECRET")
        client_id = _get_env("AZURE_CLIENT_ID")

        if identity_endpoint and identity_header:
            params = {
                "api-version": "2019-08-01",
                "resource": "https://database.windows.net/",
            }
            if client_id:
                params["client_id"] = client_id

            request = Request(
                f"{identity_endpoint}?{urlencode(params)}",
                headers={
                    "X-IDENTITY-HEADER": identity_header,
                    "Metadata": "true",
                },
                method="GET",
            )
        elif msi_endpoint and msi_secret:
            params = {
                "api-version": "2017-09-01",
                "resource": "https://database.windows.net/",
            }
            request = Request(
                f"{msi_endpoint}?{urlencode(params)}",
                headers={"Secret": msi_secret, "Metadata": "true"},
                method="GET",
            )
        else:
            raise ValueError("Managed identity endpoint is not available in environment")

        with urlopen(request, timeout=timeout_sec) as response:
            payload = json.loads(response.read().decode("utf-8"))
            token = payload.get("access_token")
            if not token:
                raise ValueError("Managed identity token response did not include access_token")
            return token
   
    is_monday = datetime.datetime.now().isoweekday() == 1
    
    if is_monday:
        query = "SELECT * FROM dbo.FinalSuggestPO ORDER BY PreVendor ASC"
    else:
        query = "SELECT * FROM dbo.FinalSuggestPO where shortqty is not null ORDER BY PreVendor ASC"


    with pytds.connect(
        server=host,
        port=port,
        database=database,
        login_timeout=timeout_sec,
        timeout=timeout_sec,
        as_dict=True,
        cafile=certifi.where(),
        validate_host=True,
        access_token_callable=_access_token_callable,
    ) as connection:
        cursor = connection.cursor()
        cursor.execute(query)
        records = list(cursor.fetchall())

    return records


def _group_rows_by_prevendor(rows: List[Dict[str, Any]]) -> Iterable[Tuple[str, List[Dict[str, Any]]]]:
    grouped: List[Dict[str, Any]] = []
    current_vendor = ""
    
    for row in rows:

        vendor = str(row.get("PreVendor", ""))
        if not grouped:
            grouped = [row]
            current_vendor = vendor
            continue

        if vendor == current_vendor:
            grouped.append(row)
            continue

        yield current_vendor, grouped
        grouped = [row]
        current_vendor = vendor

    if grouped:
        yield current_vendor, grouped



def _eval_rows(rows: List[Dict[str, Any]]):
    # eval each row against conditions and remove if any condition is met:
    
    is_monday = datetime.datetime.now().isoweekday() == 1
    week_of_month = (datetime.datetime.now().day - 1) // 7 + 1
    
    i = 1
    while i < len(rows):
        # Remove the item at index i
        prod_id = str(rows[i].get("ProdID"))
        item_rank = str(rows[i].get("Rank2"))
        schedule = str(rows[i].get("Schedule"))
        shortqty = str(rows[i].get("ShortQty"))
        monthreq = 0 if rows[i].get("BelowMthReq") is None else rows[i].get("BelowMthReq")
        minmonth = rows[i].get("MinMonth")
        logging.info("Evaluating row %s: ProdID=%s, Rank2=%s, Schedule=%s, ShortQty=%s", i, prod_id, item_rank, schedule, shortqty)
       
        if item_rank in ("E"):
            del rows[i]
            #email team about rank E items in the feed
            logging.info("Removed Rank %s,%s",prod_id, item_rank) 
        elif schedule == "999":
            del rows[i]
            logging.info("Removed Sched %s,%s", prod_id, schedule)  
        elif monthreq >= minmonth: 
            del rows[i]
            logging.info("Removed MonthReq %s,%s", prod_id, monthreq)
        elif is_monday and len(schedule) == 2:
            if schedule[0] != str(week_of_month) and schedule[1] != str(week_of_month):
                del rows[i]
                logging.info("Removed Monday Sched %s,%s,%s", prod_id, schedule, week_of_month)
            else:
                i += 1
        elif is_monday and len(schedule) == 1:
            if schedule[0] != str(week_of_month):
                del rows[i]
                logging.info("RemovedMonday Sched %s,%s,%s", prod_id, schedule, week_of_month)
            else:
                i += 1
        else:
            i += 1
    

def _payload_rows(rows: List[Dict[str, Any]]):
    # eval each row against conditions and remove if any condition is met:
    pourl = "https://10.54.9.36:5000/PurchaseOrders/",
    buyurl = "http://10.54.9.36:5000/BuyLines/"
      
    current_vendor = ""

    i = 1
    
    while i < len(rows):
        # Remove the item at index i
        prod_id = str(rows[i].get("ProdID"))
        lineqty = str(rows[i].get("LineQty"))
        shortqty = str(rows[i].get("ShortQty"))
        prevendor = str(rows[i].get("PreVendor"))
        recbranch = str(rows[i].get("Branch"))

        if  prevendor != current_vendor:
            
            current_vendor = prevendor
            payload = {
                "priceBranch": "1",
                "receiveBranch": recbranch.strip(),
                "payToVendor": "8462",  #payToId
                "shipFromVendor": "10606", #prevendor
                "orderStatus": 2,
                "writer": "API",
                "freightTermsCode": "DEL-DELIVERED",
                "isConsignmentPO": False,
                "lines": [
                    {   
                        "lineItemProduct": {
                            "productId": prod_id.strip(),
                            "quantity": lineqty,
                            "um": "",
                            "umQuantity": 1,
                        }
                    }
                ] 
            }
            response = requests.post(pourl, json=payload, verify=False)
            if response.status_code != 200:
                logging.info("Failed to create PO %s",pourl)
            else:
                new_po = response.json().get("eclipseOid")
        else:
            
            payload = {   
                        "lineItemProduct": {
                            "productId": prod_id.strip(),                  
                            "quantity": lineqty,
                            "um": "",
                            "umQuantity": 1
                        }
                    }  
            url = pourl + str(new_po) + "/Lines?generationId=1"
            response = requests.post(url, json=payload, verify=False)
            url=""
            if response.status_code != 200:
                logging.info("Failed to send payload for ProdID %s to %s. Output: %s", prod_id, url, json.dumps(payload, ensure_ascii=True, default=str))
            else:
                returndata = response.json()
                print (returndata)
        i +=1    

def _output_batches(
    source_table: str,
    prevendor: str,
    rows: List[Dict[str, Any]],
    batch_size: int,
    batch_start_number: int,
) -> None:
    for offset, batch in enumerate(_chunked(rows, batch_size), start=0):
        batch_number = batch_start_number + offset
        #
        #logging.info("Batch output: %s", json.dumps(payload, ensure_ascii=True, default=str))


def process_table_records() -> int:
    sql_connection_string = _get_env("SQL_CONNECTION_STRING")
    batch_size = _get_int_env("BATCH_SIZE", 100)
    timeout_sec = _get_int_env("SQL_TIMEOUT_SEC", 30)

    if not sql_connection_string:
        raise ValueError("SQL_CONNECTION_STRING is required")

    rows = _fetch_records_from_sql(sql_connection_string, timeout_sec)
    if not rows:
        logging.info("No records found in dbo.FinalSuggestPO")
        return 0

    logging.info("Fetched %s records from dbo.FinalSuggestPO", len(rows))
    
    _eval_rows(rows)
    
    logging.info("%s records remain after evaluation", len(rows))

    if not rows:
        logging.info("All records failed eval in dbo.FinalSuggestPO")
        return 0

    total_rows_sent = 0
    next_batch_number = 1

    _payload_rows(rows)
   
    for vendor, vendor_rows in _group_rows_by_prevendor(rows):
        _output_batches(
            source_table="dbo.FinalSuggestPO",
            prevendor=vendor,
            rows=vendor_rows,
            batch_size=batch_size,
            batch_start_number=next_batch_number,
        )
        next_batch_number += (len(vendor_rows) + batch_size - 1) // batch_size
        total_rows_sent += len(vendor_rows)

    logging.info("Completed ingestion for dbo.FinalSuggestPO")
    return total_rows_sent


@app.function_name(name="csv_ingest_timer")
@app.schedule(schedule="%CSV_INGEST_SCHEDULE%", arg_name="mytimer", run_on_startup=False, use_monitor=True)
def csv_ingest_timer(mytimer: func.TimerRequest) -> None:
    if mytimer.past_due:
        logging.warning("The CSV ingest timer is running late")

    start = time.time()
    rows = process_table_records()
    elapsed = round(time.time() - start, 2)
    logging.info("CSV ingestion run finished. rows=%s elapsed_seconds=%s", rows, elapsed)


@app.function_name(name="csv_ingest_http")
@app.route(route="ingest", methods=["GET", "POST"], auth_level=func.AuthLevel.FUNCTION)
def csv_ingest_http(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Received manual CSV ingest request via HTTP trigger")

    try:
        start = time.time()
        rows = process_table_records()
        elapsed = round(time.time() - start, 2)
        body = {
            "status": "success",
            "rowsProcessed": rows,
            "elapsedSeconds": elapsed,
            "message": "CSV ingestion completed successfully",
        }
        return func.HttpResponse(
            body=json.dumps(body),
            status_code=200,
            mimetype="application/json",
        )
    except Exception as exc:
        logging.exception("Manual CSV ingestion failed")
        body = {
            "status": "error",
            "message": "CSV ingestion failed",
            "details": str(exc),
        }
        return func.HttpResponse(
            body=json.dumps(body),
            status_code=500,
            mimetype="application/json",
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    process_table_records()
