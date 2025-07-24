import time
import threading
import datetime
import yaml
import os
from dataclasses import dataclass
from enum import StrEnum, auto
from typing import Any, Optional, List, Dict
from pathlib import Path
from ftfcu_appworx import Apwx, JobTime
from oracledb import Connection as DbConnection
from datetime import datetime, timezone
from multiprocessing import Manager
import pyodbc
import re


version = 1.00

TITLE_FORMAT = "{:>90}"
LINE_FORMAT = "{:<20}"


class AppWorxEnum(StrEnum):
    TNS_SERVICE_NAME = auto()
    CONFIG_FILE_PATH = auto()
    OUTPUT_FILE_NAME = auto()
    OUTPUT_FILE_PATH = auto()
    TEST_YN = auto()
    DEBUG_YN = auto()
    MAX_THREADS = auto()
    MODE = auto()
    P2P_SERVER = auto()
    P2P_SCHEMA = auto()
    RPTONLY_YN = auto()
    OLD_ZOE_FILE = auto()
    NEW_ZOE_FILE = auto()

    def _str_(self):
        return self.name


@dataclass
class ScriptData:
    apwx: Apwx
    dbh: DbConnection
    config: Any


def run(apwx: Apwx) -> bool:
    """Main function to control execution based on mode"""
    print("run started")
    script_data = initialize(apwx)
    mode = apwx.args.MODE.upper()

    if mode not in ("NEW", "DELTA"):
        raise ValueError("Invalid MODE. Must be 'NEW' or 'DELTA'.")

    print(f"ZOE file mode is {mode}")
    fh_zoe_path = os.path.join(apwx.args.OUTPUT_FILE_PATH, apwx.args.OUTPUT_FILE_NAME)

    if mode == "NEW":
        return process_new_mode(apwx, script_data, fh_zoe_path)
    elif mode == "DELTA":
        return process_delta_mode(apwx, fh_zoe_path)
    else:
        print("MODE IS NOT NEW OR DELTA")
        return False


def process_new_mode(apwx, script_data, fh_zoe_path: str) -> bool:
    """Handles the NEW mode logic by collecting ZOE records using threading and writing to a file"""
    file_stat = get_file_stat_if_exists(fh_zoe_path)

    zoe_data = collect_zoe_records_multithreaded(apwx, script_data)
    print(f"Found {len(zoe_data)} ZOE records")

    write_new_mode_file(fh_zoe_path, zoe_data, apwx, file_stat)
    return True


def collect_zoe_records_multithreaded(apwx, script_data) -> List[str]:
    """use multiple threads to fetch ZOE records in parallel and combine them into a single list"""
    threads_list = []
    manager = Manager()
    zoe_data = manager.list()
    max_threads = int(apwx.args.MAX_THREADS)

    for thread_id in range(max_threads):
        thread = threading.Thread(
            target=thread_sub,
            args=(
                thread_id + 1,
                script_data,
                apwx,
                thread_id,
                max_threads,
                zoe_data
            ),
        )
        threads_list.append(thread)
        thread.start()

    for thread in threads_list:
        thread.join()

    return list(zoe_data)


def write_new_mode_file(file_path: str, records: List[str], apwx, file_stat):
    """writes header, details, trailers records to a file for new mode after cleaning and formatting the data"""
    seq_nbr = 0
    added = 0
    acct_hash = 0

    with open(file_path, "w", encoding="utf-8") as f:
        f.write(build_cde_record() + "\n")
        f.write(build_header_record({"test": apwx.args.TEST_YN, "fileType": "LOAD"}) + "\n")

        print("Writing detail records")
        for record in records:
            clean_record = clean_record_report(record)
            fields = clean_record.split("|")

            if len(fields) > 3:  # Ensure record has at least 4 fields (expected format)
                acct_hash += safe_int(fields[3])

            detail_record = build_detail_report(seq_nbr + 1, fields, apwx.args.TEST_YN)
            seq_nbr += 1
            added += 1
            f.write(detail_record + "\n")

        trailer = build_trailer_record({
            "record_ct": len(records) + 2,
            "added": added,
            "changed": 0,
            "deleted": 0,
            "acctHash": acct_hash,
            "test": apwx.args.TEST_YN,
            "fileType": "LOAD",
        }, file_stat)
        f.write(trailer + "\n")


def process_delta_mode(apwx, file_path: str) -> bool:
    """Handles DELTA mode logic by comparing new and old files and writing difference to output file"""
    seq_nbr = 0
    added = 0
    changed = 0
    deleted = 0

    with open(file_path, "w", encoding="utf-8") as f:
        f.write(build_cde_record() + "\n")
        f.write(build_header_record({"test": apwx.args.TEST_YN, "fileType": "UPDT"}) + "\n")
        file_stat = get_file_stat_if_exists(file_path)
        hash_old, _ = get_zoe_file_hash(apwx.args.OLD_ZOE_FILE)
        hash_new, acct_hash_new = get_zoe_file_hash(apwx.args.NEW_ZOE_FILE)
        print("Comparing New to Old")
        for key, new_rec in hash_new.items():
            if key in hash_old:
                # Start sequence number from 1 for file line records (not zero-based)
                line_seq_nbr = seq_nbr + 1
                if new_rec != hash_old[key]:
                    f.write(build_delta_detail_report(line_seq_nbr, new_rec, 'C', apwx.args.TEST_YN) + "\n")
                    seq_nbr += 1
                    changed += 1
            else:
                f.write(build_delta_detail_report(line_seq_nbr, new_rec, 'A', apwx.args.TEST_YN) + "\n")
                seq_nbr += 1
                added += 1

        trailer = build_trailer_record({
            "record_ct": seq_nbr + 2,
            "added": added,
            "changed": changed,
            "deleted": deleted,
            "acctHash": acct_hash_new,
            "test": apwx.args.TEST_YN,
            "fileType": "UPDT",
        }, file_stat)

        f.write(trailer + "\n")

    return True


def clean_record_report(record: str) -> str:
    """removing excessive tabs"""
    return re.sub(r"\t+", " ", str(record).strip())


def build_detail_report(seq: int, fields: List[str], test_yn: str) -> str:
    """builds a formatted detail record string from field data for new mode"""
    header_fields = {
        "record_type": "6",
        "action": "A",
        "mode": "03" if test_yn == "Y" else "01",
        "source": "FTF",
        "sequence": str(seq),
    }
    prefix = "|".join(header_fields.values())
    detail_data = "|".join(fields[:56])  # first 56 fields are required in the detailed record format.
    return f"{prefix}|{detail_data}"


def build_delta_detail_report(seq: int, record: str, action: str, test_yn: str) -> str:
    """formatted line for delta mode with an action type (a/c)"""
    header_fields = {
        "record_type": "6",
        "action": action,
        "mode": "03" if test_yn == "Y" else "01",
        "source": "FTF",
        "sequence": str(seq),
    }
    prefix = "|".join(header_fields.values())
    return f"{prefix}|{record}"


def safe_int(val: str) -> int:
    """converts a string to integer safely"""
    return int(val) if val and val.isdigit() else 0


def get_file_stat_if_exists(file_path: str):
    """returns file data if the file exists, else returns none"""
    if os.path.exists(file_path):
        return os.stat(file_path)
    else:
        print(f"File not found: {file_path}")
        return None


def thread_sub(
    connection_num: int,
    script_data,
    apwx: Apwx,
    thread_id: int,
    max_threads: int,
    zoe_data: list,

):
    """run by each thread to handle database connections and process zoe records specific to its thread id"""
    time.sleep(connection_num)
    print(f"Started thread: {thread_id}")

    p2p_args = {
        "zoe": True,
        "storeApwx": "zoe",
        "getDnaDb": True,
        "getP2pDb": True,
        "maxThread": max_threads,
        "threadId": thread_id,
        "p2pServer": apwx.args.P2P_SERVER,
        "p2pSchema": apwx.args.P2P_SCHEMA,
        "storeDbh": "zoe",
    }

    p2p_db_connect = p2p_db_connect_func(p2p_args)

    dna_db_connect = dna_db_connect_func(apwx)

    process_zoe_records(dna_db_connect, p2p_db_connect, script_data, max_threads, thread_id, zoe_data)

    if dna_db_connect:
        dna_db_connect.close()

    print(f"Finished thread: {thread_id}")


def load_p2p_customers(p2p_dbh, script_data):
    """Fetch and return a dictionary of P2P customer records key by 'persnbr'."""
    p2p_cust = {}
    if p2p_dbh:
        try:
            p2p_records = execute_sql_select(p2p_dbh, script_data.config["p2p_cust_org"])
            for record in p2p_records:
                persnbr = record["persnbr"]
                if persnbr:
                    p2p_cust[persnbr] = record
        except Exception as e:
            print(f"Error fetching P2P customer data: {e}")
    return p2p_cust


def process_query_key(key, dbh, sql, p2p_cust, zoe_data, thread_id, render_values):
    """Execute a query and process its result set."""
    cur = dbh.cursor()
    try:
        if key == "p2p_cust_org":
            cur.execute(sql)
        else:
            cur.execute(sql, render_values)

        max_rows = 1000
        while True:
            records = cur.fetchmany(max_rows)
            if not records:
                break
            for record in records:
                record_list = list(record)
                is_org = key in ["card_own_pers_org", "org"]
                line = build_detail_record(record_list, p2p_cust, is_org)
                if line:
                    zoe_data.append(line)

        print(f"[THREAD {thread_id}] Processed records from '{key}'.")

    except Exception as e:
        print(f"[THREAD {thread_id}] Error processing query '{key}': {e}")
    finally:
        if cur:
            cur.close()


def process_zoe_records(dna_dbh, p2p_dbh, script_data, max_thread, thread_id, zoe_data):
    """Main entry point to process ZOE records."""
    p2p_cust = load_p2p_customers(p2p_dbh, script_data)
    render_values = {"max_thread": max_thread, "thread_id": thread_id}

    query_keys = [
        "card_tax_rpt_for_pers",
        "card_own_pers",
        "no_card_tax_rpt_for_pers",
        "no_card_own_pers",
        "card_own_pers_org",
        "org",
        "p2p_cust_org",
    ]

    for key in query_keys:
        dbh = p2p_dbh if key == "p2p_cust_org" else dna_dbh

        if key == "org" or key == "p2p_cust_org":
            sql = script_data.config[key]
        else:
            sql = script_data.config["sql_qq"] + "\n" + script_data.config[key]

        process_query_key(key, dbh, sql, p2p_cust, zoe_data, thread_id, render_values)


def build_detail_record(record_ary: List, p2p_cust: Dict, is_org: bool = False) -> str:
    """Build detail record from database record"""

    if len(record_ary) < 2:
        return ""

    persnbr = record_ary[1] if len(record_ary) > 1 else None
    line_ary = record_ary[0:2]  # Initialize line with first two fields (e.g., account number and person number)

    if not is_org and persnbr in p2p_cust and p2p_cust[persnbr].get("CXCCustomerID"):
        line_ary.append(p2p_cust[persnbr]["CXCCustomerID"])
    else:
        line_ary.append(persnbr)

    # Add fields from index 2 to 12 if they exist, otherwise pad with empty strings
    if len(record_ary) > 12:
        line_ary.extend(record_ary[2:13])
    else:
        line_ary.extend([""] * (13 - len(line_ary)))

    # Add registered email and flag if available in p2p_cust, else fallback to field 13 and flag 0
    if not is_org and persnbr in p2p_cust and p2p_cust[persnbr].get("registeredEmail"):
        line_ary.append(p2p_cust[persnbr]["registeredEmail"])
        line_ary.append(1)
    else:
        line_ary.append(record_ary[13] if len(record_ary) > 13 else "")
        line_ary.append(0)

    # Add fields 14 and 15 (if exist), else empty strings
    if len(record_ary) > 15:
        line_ary.extend(record_ary[14:16])
    else:
        line_ary.extend(["", ""])

    # Extract and append up to 6 ID fields from index 16 using parse_id function
    id_ary = parse_id(record_ary[16] if len(record_ary) > 16 else "", is_org)
    line_ary.extend(id_ary[0:6])  # Adds fields like CDE0166 to CDE0206

    # Add fields from index 17 to 22 (if exist), else add 6 empty strings
    if len(record_ary) > 22:
        line_ary.extend(record_ary[17:23])
    else:
        line_ary.extend([""] * 6)

    # Add registered phone and flag if available in p2p_cust, else fallback to field 23 and flag 0
    if not is_org and persnbr in p2p_cust and p2p_cust[persnbr].get("registeredPhone"):
        line_ary.append(p2p_cust[persnbr]["registeredPhone"])
        line_ary.append(1)
    else:
        line_ary.append(record_ary[23] if len(record_ary) > 23 else "")
        line_ary.append(0)

    # Add fields from index 24 to 47 (if available), otherwise pad with empty strings to fill up to index 47
    if len(record_ary) > 47:
        line_ary.extend(record_ary[24:48])
    else:
        remaining_fields = 48 - len(line_ary)
        if remaining_fields > 0:
            line_ary.extend([""] * remaining_fields)

    # Append field at index 49 (if exists), else add an empty string
    if len(record_ary) > 49:
        line_ary.append(record_ary[49])
    else:
        line_ary.append("")

    return "|".join(str(val) if val is not None else "" for val in line_ary)


def parse_id(id_record_str: str, is_org: bool = False) -> List[str]:
    """Parse ID string into list of up to 6 fields"""

    id_ary = []

    if not is_org and id_record_str:
        if "|" in id_record_str:
            id_row_ary = [row.split(":") for row in id_record_str.split("|")]
        else:
            id_row_ary = [id_record_str.split(":")]

        # Filter for USA IDs based on 4th field
        usa_id_ary = [row for row in id_row_ary if len(row) > 3 and row[3] == "USA"]

        if usa_id_ary:
            for us_id in usa_id_ary:
                if len(us_id) > 4 and us_id[4]:  # Ensure the 5th field exists and is not empty
                    id_ary = us_id[:6]  # Use first 6 fields from USA ID
                    break
        else:
            foreign_id_ary = [row for row in id_row_ary if len(row) > 3 and row[3] != "USA"]
            for for_id in foreign_id_ary:
                if len(for_id) > 4 and for_id[4]:
                    id_ary = for_id[:6]
                    break

    # Pad with empty strings if fewer than 6 fields
    while len(id_ary) < 6:
        id_ary.append("")

    return id_ary[:6]


def build_header_record(args: Dict) -> str:
    """header record string based on file type and test flag"""
    header_rec_ary = []
    header_rec_ary.append("1")
    header_rec_ary.append(args["fileType"])
    header_rec_ary.append("03" if args["test"] == "Y" else "01")
    header_rec_ary.append("FTF")

    return "|".join(header_rec_ary)


def build_trailer_record(args: Dict, file_stat=None) -> str:
    """builds a trailer record with file summary and timestamp"""
    if file_stat is None:
        file_epoch = int(time.time())
    else:
        file_epoch = int(file_stat.st_mtime)

    file_create_date = datetime.now().strftime("%Y%m%d")
    file_create_time = datetime.fromtimestamp(file_epoch).strftime("%H%M%S")
    file_ms = datetime.fromtimestamp(file_epoch).microsecond // 1000

    if "record_ct" not in args:
        raise ValueError("Record Count argument is undefined")
    if "acctHash" not in args:
        raise ValueError("Account Hash argument is undefined")

    file_acct_hash = args["acctHash"]
    file_record_ct = args["record_ct"]
    file_add_ct = args.get("added", 0)
    file_change_ct = args.get("changed", 0)
    file_delete_ct = args.get("deleted", 0)

    trailer_ary = ["9", args["fileType"], "03" if args["test"] == "Y" else "01", "FTF"]

    cde_vals = [
        "CDE0083",
        "CDE0084",
        "CDE0110",
        "CDE0111",
        "CDE0120",
        "CDE0121",
        "CDE0123",
        "CDE0133",
        "CDE0139",
        "CDE0151",
        "CDE0165",
        "CDE0418",
        "CDE0419",
        "CDE0429",
        "CDE0430",
        "CDE0467",
        "CDE0674",
        "CDE0676",
        "CDE0811",
    ]

    trailer_vals = [
        file_create_date,
        f"{file_create_time}{file_ms}",
        file_acct_hash,
        file_add_ct,
        file_change_ct,
        file_delete_ct,
        "",
        file_record_ct,
        "ZOE",
        "",  # FI bank id
        "",  # FI region
        "",  # xfer date
        "",  # xfer time
        "",  # process end date
        "",  # process end time
        file_epoch,
        "",  # source file name
        "A",
        "",  # client id
    ]

    cde_pairs = [f"{cde_vals[i]}:{trailer_vals[i]}" for i in range(len(cde_vals))]
    trailer_ary.extend(cde_pairs)

    return "|".join(str(val) for val in trailer_ary)


def get_zoe_file_hash(file_path: str) -> tuple:
    """Get hash of ZOE file records"""
    hash_zoe = {}
    acct_hash = 0

    try:
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                lines = f.readlines()
        except UnicodeDecodeError:
            with open(file_path, "r", encoding="latin-1") as f:
                lines = f.readlines()

        for line in lines:
            line = line.strip()
            if line and not line.startswith("CDE") and "|" in line:
                parts = line.split("|")
                if len(parts) > 6:
                    key = parts[6] if len(parts) > 6 else line
                    record_data = "|".join(parts[5:]) if len(parts) > 5 else line
                    hash_zoe[key] = record_data

                    if len(parts) > 6 and parts[6].isdigit():
                        acct_hash += int(parts[6])
    except FileNotFoundError:
        print(f"File not found: {file_path}")
    except Exception as e:
        print(f"Error reading file {file_path}: {e}")

    return hash_zoe, acct_hash


def p2p_db_connect_func(args: dict):
    """connects to p2p sql server database"""
    p2p_server = args.get("p2pServer")
    p2p_schema = args.get("p2pSchema")
    driver_name = "SQL Server"

    dsn = (
        f"driver={{{driver_name}}};"
        f"SERVER={p2p_server};"
        f"DATABASE={p2p_schema};"
        f"Trusted_Connection=yes;"
    )

    try:
        dbh = pyodbc.connect(dsn)
        return dbh
    except Exception as e:
        print(f"Failed to connect to P2P DB: {e}")
        return None


def dna_db_connect_func(apwx: Apwx):
    """connects to dna database"""
    try:
        dbh = apwx.db_connect(autocommit=False)
        print("[DNA DB CONNECTED]")
        return dbh
    except Exception as e:
        print(f"Failed to connect to DNA DB: {e}")
        return None


def execute_sql_select(conn, sql: str) -> List[Dict]:
    """runs the query and returns results as a list of dist"""
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            cols = [desc[0] for desc in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchmany()]
    except Exception as e:
        print(f"SQL execution error: {e}")
        return []


def build_cde_record() -> str:
    """returns a pipe separated string of CDE field codes"""
    return "|".join(

        [
            "CDE0380", "CDE0377", "CDE0276", "CDE0157", "CDE0557",
            "CDE0014", "CDE0011", "CDE1023", "CDE0019", "CDE1024",
            "CDE1025", "CDE0023", "CDE0029", "CDE0032", "CDE0033",
            "CDE0036", "CDE0055", "CDE0056", "CDE0077", "CDE0100",
            "CDE1026", "CDE0141", "CDE0145", "CDE0166", "CDE0175",
            "CDE0182", "CDE0192", "CDE0199", "CDE0206", "CDE0215",
            "CDE0216", "CDE0219", "CDE0222", "CDE0227", "CDE0233",
            "CDE0277", "CDE1027", "CDE0238", "CDE0283", "CDE0284",
            "CDE0290", "CDE0299", "CDE0309", "CDE0319", "CDE0320",
            "CDE0321", "CDE0322", "CDE0323", "CDE0324", "CDE0334",
            "CDE0345", "CDE0354", "CDE0408", "CDE0409", "CDE0802",
            "CDE1275", "CDE1271", "CDE1272", "CDE1273", "CDE1274",
            "CDE0010"
        ]

    )


def initialize(apwx: Apwx) -> ScriptData:
    """initialize database connection"""
    dbh = apwx.db_connect(autocommit=False)
    config = get_config(apwx)
    return ScriptData(apwx=apwx, dbh=dbh, config=config)


def get_config(apwx: Apwx) -> Any:
    """loads the config file"""
    with open(apwx.args.CONFIG_FILE_PATH, "r") as f:
        return yaml.safe_load(f)


def get_apwx() -> Apwx:
    return Apwx(["OSIUPDATE", "OSIUPDATE_PW"])


def parse_args(apwx: Apwx) -> Apwx:
    """parses command line arguments using apwx"""
    parser = apwx.parser
    parser.add_arg(AppWorxEnum.TNS_SERVICE_NAME, type=str, required=True)
    parser.add_arg(AppWorxEnum.CONFIG_FILE_PATH, type=r"(.yml|.yaml)$", required=True)
    parser.add_arg(AppWorxEnum.OUTPUT_FILE_NAME, type=str, required=True)
    parser.add_arg(
        AppWorxEnum.OUTPUT_FILE_PATH, type=parser.dir_validator, required=True
    )
    parser.add_arg(AppWorxEnum.TEST_YN, choices=["Y", "N"], default="N", required=False)
    parser.add_arg(
        AppWorxEnum.DEBUG_YN, choices=["Y", "N"], default="N", required=False
    )
    parser.add_arg(AppWorxEnum.MAX_THREADS, type=str, required=True)
    parser.add_arg(AppWorxEnum.MODE, type=str, required=True)
    parser.add_arg(AppWorxEnum.P2P_SERVER, type=str, required=True)
    parser.add_arg(AppWorxEnum.P2P_SCHEMA, type=str, required=True)
    parser.add_arg(
        AppWorxEnum.RPTONLY_YN, choices=["Y", "N"], default="Y", required=False
    )

    parser.add_arg(AppWorxEnum.OLD_ZOE_FILE, type=str, required=False)
    parser.add_arg(AppWorxEnum.NEW_ZOE_FILE, type=str, required=False)

    apwx.parse_args()
    return apwx


if _name_ == "_main_":
    JobTime().print_start()
    run(parse_args(get_apwx()))
    JobTime().print_end()
