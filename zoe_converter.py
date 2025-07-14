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
import pytz
import pyodbc
import re
import stat

version = 1.00

TITLE_FORMAT = "{:>90}"
LINE_FORMAT = "{:<20}"


class AppWorxEnum(StrEnum):
    TNS_SERVICE_NAME = "DNATST4"
    CONFIG_FILE_PATH = "config.yaml"
    OUTPUT_FILE_NAME = "AOEP2P01.FTF"
    OUTPUT_FILE_PATH = r'C:\Users\saitrinadhk\Documents\output\paymentupdate'
    TEST_YN = "N"
    DEBUG_YN = "N"
    MAX_THREADS = "8"
    MODE = "NEW"
    P2P_SERVER = "P2PPRODLS,58318"
    P2P_SCHEMA = "P2P"
    P2P_DRIVERNAME = "SQL"
    HOST = "localhost"
    SID = "DNATST4"
    RPT_ONLY = "N"

    def __str__(self):
        return self.name


@dataclass
class ScriptData:
    apwx: Apwx
    dbh: DbConnection
    config: Any


def run(apwx: Apwx, current_time: float) -> bool:
    """Main execution function"""
    script_data = initialize(apwx)
    print("apwx: ", apwx)
    print("Script_data: ", script_data)
    mode = apwx.args.MODE
    
    if mode not in ("NEW", "DELTA"):
        raise ValueError("Invalid MODE. Must be 'NEW' or 'DELTA'.")
    print(f"ZOE file mode is {mode}")

    fh_zoe_path = os.path.join(apwx.args.OUTPUT_FILE_PATH, apwx.args.OUTPUT_FILE_NAME)
    
    with open(fh_zoe_path, "w", encoding="utf-8") as f:
        f.write(build_cde_record() + "\n")

    seq_nbr = 0
    added = 0
    changed = 0
    deleted = 0
    acct_hash = 0

    if mode == "NEW":
        try:
            file_stat = os.stat(fh_zoe_path)
            print(f"File stats: {file_stat}")
        except FileNotFoundError:
            print(f"File not found: {fh_zoe_path}")
            file_stat = None

        threads_list = []
        manager = Manager()
        zoe_data = manager.list()  # Shared list among threads
        max_threads = int(apwx.args.MAX_THREADS)
        connection_num = 0

        print("Fetching ZOE records from DNA")

        for thread_id in range(max_threads):
            apwx_t = apwx  # clone if needed; here it's just passed
            connection_num += 1
            thread = threading.Thread(
                target=thread_sub,
                args=(connection_num, apwx_t, thread_id, max_threads, zoe_data, apwx)
            )
            threads_list.append(thread)
            thread.start()

        for thread in threads_list:
            thread.join()

        print(f"Found {len(zoe_data)} ZOE records")

        # Reopen file and write header and records
        with open(fh_zoe_path, "w", encoding="utf-8") as f:
            f.write(build_cde_record() + "\n")
            
            header_rec = build_header_record({
                'test': apwx.args.TEST_YN, 
                'fileType': 'LOAD'
            })
            f.write(header_rec + "\n")

            print('Printing ZOE file')
            
            for record in zoe_data:
                record = str(record).strip()
                record = re.sub(r'\t+', ' ', record)  # Replace tabs with spaces
                
                line_ary = record.split('|')
                if len(line_ary) > 3:
                    acct_hash += int(line_ary[3]) if line_ary[3].isdigit() else 0
                
                # Remove the last element (account status) from line_ary
                acct_stat = line_ary.pop() if line_ary else ""
                
                detail_first5 = "|".join([
                    '6',
                    'A',
                    '03' if apwx.args.TEST_YN == 'Y' else '01',
                    'FTF',
                    str(seq_nbr + 1)
                ])
                seq_nbr += 1
                added += 1
                
                # Join first 56 elements (0-55) of line_ary
                line = "|".join([detail_first5] + line_ary[:56])
                f.write(line + "\n")

            trailer_rec = build_trailer_record({
                'recordCt': len(zoe_data) + 2,  # +2 for header/trailer
                'added': added,
                'changed': changed,
                'deleted': deleted,
                'test': apwx.args.TEST_YN,
                'fileType': 'LOAD',
                'acctHash': acct_hash,
            }, file_stat)
            
            f.write(trailer_rec + "\n")

    elif mode == "DELTA":
        # Delta mode implementation
        print("Processing DELTA mode")
        
        with open(fh_zoe_path, "w", encoding="utf-8") as f:
            f.write(build_cde_record() + "\n")
            
            header_rec = build_header_record({
                'test': apwx.args.TEST_YN,
                'fileType': 'UPDT'
            })
            f.write(header_rec + "\n")
            
            file_stat = os.stat(fh_zoe_path)
            
            # Read old and new ZOE files and compare
            hash_zoe_old, _ = get_zoe_file_hash(apwx.args.OLD_ZOE_FILE)
            hash_zoe_new, acct_hash = get_zoe_file_hash(apwx.args.NEW_ZOE_FILE)
            
            print("Comparing New to Old")
            rec_ct = 0
            
            for k, new_record in hash_zoe_new.items():
                if k in hash_zoe_old:
                    if new_record != hash_zoe_old[k]:
                        # Record has changed
                        detail_first5 = "|".join([
                            '6', 'C',
                            '03' if apwx.args.TEST_YN == 'Y' else '01',
                            'FTF', str(seq_nbr + 1)
                        ])
                        seq_nbr += 1
                        changed += 1
                        rec_ct += 1
                        
                        line_ary = new_record.split('|')
                        if len(line_ary) > 3:
                            acct_hash += int(line_ary[3]) if line_ary[3].isdigit() else 0
                        
                        f.write(f"{detail_first5}|{new_record}\n")
                else:
                    # New record
                    detail_first5 = "|".join([
                        '6', 'A',
                        '03' if apwx.args.TEST_YN == 'Y' else '01',
                        'FTF', str(seq_nbr + 1)
                    ])
                    seq_nbr += 1
                    added += 1
                    rec_ct += 1
                    
                    line_ary = new_record.split('|')
                    if len(line_ary) > 3:
                        acct_hash += int(line_ary[3]) if line_ary[3].isdigit() else 0
                    
                    f.write(f"{detail_first5}|{new_record}\n")
            
            trailer_rec = build_trailer_record({
                'test': apwx.args.TEST_YN,
                'fileType': 'UPDT',
                'added': added,
                'changed': changed,
                'deleted': deleted,
                'acctHash': acct_hash,
                'recordCt': rec_ct + 2,  # +2 for header/trailer
            }, file_stat)
            
            f.write(trailer_rec + "\n")

    return True


def thread_sub(connection_num: int, apwx: Apwx, thread_id: int, max_threads: int, zoe_data: list, apwx_vars: Apwx):
    """Thread function to process ZOE records"""
    time.sleep(connection_num)  # Delay to stagger thread starts
    print(f"Started thread: {thread_id}")
    
    p2p_args = {
        'zoe': True,
        'storeApwx': 'zoe',
        'getDnaDb': True,
        'getP2pDb': True,
        'maxThread': max_threads,
        'threadId': thread_id,
        'host': apwx.args.HOST,
        'sid': apwx.args.SID,
        'user': apwx.OSIUPDATE,
        'pw': apwx.OSIUPDATE_PW,
        'p2pServer': apwx.args.P2P_SERVER,
        'p2pSchema': apwx.args.P2P_SCHEMA,
        'p2pDriverName': apwx.args.P2P_DRIVERNAME,
        'storeDbh': 'zoe',
    }

    # Connect to P2P database
    p2p_db_connect = p2p_db_connect_func(p2p_args)
    print("p2p: ", p2p_db_connect)

    # Connect to DNA database
    dna_db_connect = dna_db_connect_func(p2p_args, apwx)
    print("dna_db_connect: ", dna_db_connect)

    # Process ZOE records
    process_zoe_records(dna_db_connect, p2p_db_connect, max_threads, thread_id, zoe_data, apwx)
    
    # Close connections
    if dna_db_connect:
        dna_db_connect.close()
    
    print(f"Finished thread: {thread_id}")


def process_zoe_records(dna_dbh: DbConnection, p2p_dbh, max_thread: int, thread_id: int, zoe_data: list, apwx: Apwx):
    """Process ZOE records from database queries"""
    script_data = initialize(apwx)
    
    # Get P2P customer data first
    p2p_cust = {}
    if p2p_dbh:
        try:
            p2p_records = execute_sql_select(p2p_dbh, script_data.config["p2pCustOrg"])
            for record in p2p_records:
                p2p_cust[record.get('persnbr')] = record
        except Exception as e:
            print(f"Error fetching P2P customer data: {e}")
    
    render_values = {"max_thread": max_thread, "thread_id": thread_id}
    max_rows = 1000

    # List of config keys for each SQL query
    query_keys = [
        "cardTaxRptForPers",
        "cardOwnPers", 
        "noCardTaxRptForPers",
        "noCardOwnPers",
        "cardOwnPersOrg",
        "org"
    ]

    for key in query_keys:
        try:
            sql = script_data.config["sql_qq"] + "\n" + script_data.config[key]
            
            with dna_dbh.cursor() as cur:
                cur.execute(sql, render_values)
                
                while True:
                    records = cur.fetchmany(max_rows)
                    if not records:
                        break
                        
                    for record in records:
                        # Convert record tuple to list for processing
                        record_list = list(record)
                        is_org = key in ["cardOwnPersOrg", "org"]
                        line = build_detail_record(record_list, p2p_cust, is_org)
                        if line:
                            zoe_data.append(line)
                            
                print(f"[THREAD {thread_id}] Processed records from '{key}'.")
                
        except Exception as e:
            print(f"[THREAD {thread_id}] Error processing query '{key}': {e}")


def build_detail_record(record_ary: List, p2p_cust: Dict, is_org: bool = False) -> str:
    """Build detail record from database record"""
    if len(record_ary) < 2:
        return ""
        
    persnbr = record_ary[1] if len(record_ary) > 1 else None
    line_ary = record_ary[0:2]  # cardnbr, persnbr

    # cxcCustomerId
    if not is_org and persnbr in p2p_cust and p2p_cust[persnbr].get("CXCCustomerID"):
        line_ary.append(p2p_cust[persnbr]["CXCCustomerID"])
    else:
        line_ary.append(persnbr)

    # acctnbr thru CDE0077 (index 2 to 12 inclusive)
    if len(record_ary) > 12:
        line_ary.extend(record_ary[2:13])
    else:
        # Pad with empty strings if not enough data
        line_ary.extend([''] * (13 - len(line_ary)))

    # registeredEmail and boolean
    if not is_org and persnbr in p2p_cust and p2p_cust[persnbr].get("registeredEmail"):
        line_ary.append(p2p_cust[persnbr]["registeredEmail"])
        line_ary.append(1)
    else:
        line_ary.append(record_ary[13] if len(record_ary) > 13 else "")
        line_ary.append(0)

    # routing number CDE0141 and aba number CDE0145 (index 14,15)
    if len(record_ary) > 15:
        line_ary.extend(record_ary[14:16])
    else:
        line_ary.extend(['', ''])

    # parse ID details from field 16
    id_ary = parse_id(record_ary[16] if len(record_ary) > 16 else "", is_org)
    line_ary.extend(id_ary[0:6])  # CDE0166 - CDE0206

    # D.O.B. - middle name (index 17 to 22 inclusive)
    if len(record_ary) > 22:
        line_ary.extend(record_ary[17:23])
    else:
        # Pad with empty strings
        line_ary.extend([''] * 6)

    # registeredPhone and boolean
    if not is_org and persnbr in p2p_cust and p2p_cust[persnbr].get("registeredPhone"):
        line_ary.append(p2p_cust[persnbr]["registeredPhone"])
        line_ary.append(1)
    else:
        line_ary.append(record_ary[23] if len(record_ary) > 23 else "")
        line_ary.append(0)

    # CDE0238 - CDE1274 (index 24 to 47 inclusive)
    if len(record_ary) > 47:
        line_ary.extend(record_ary[24:48])
    else:
        # Pad remaining fields
        remaining_fields = 48 - len(line_ary)
        if remaining_fields > 0:
            line_ary.extend([''] * remaining_fields)

    # CDE0010 curracctstatcd (index 49)
    if len(record_ary) > 49:
        line_ary.append(record_ary[49])
    else:
        line_ary.append('')

    # Join with pipe, converting None to empty string
    return '|'.join(str(val) if val is not None else '' for val in line_ary)


def parse_id(id_record_str: str, is_org: bool = False) -> List[str]:
    """Parse ID record string into components"""
    id_ary = []
    
    if not is_org and id_record_str:
        id_row_ary = []
        
        if '|' in id_record_str:
            id_row_ary = [row.split(':') for row in id_record_str.split('|')]
        else:
            id_row_ary = [id_record_str.split(':')]
        
        # Filter for USA issued IDs
        usa_id_ary = [row for row in id_row_ary if len(row) > 3 and row[3] == 'USA']
        
        # Use a US issued ID if one exists
        if usa_id_ary:
            for us_id in usa_id_ary:
                if len(us_id) > 4 and us_id[4]:  # has non-null ID number
                    id_ary = us_id[:6]  # Take first 6 elements
                    break
        else:
            # Use foreign ID
            foreign_id_ary = [row for row in id_row_ary if len(row) > 3 and row[3] != 'USA']
            for for_id in foreign_id_ary:
                if len(for_id) > 4 and for_id[4]:  # has non-null ID number
                    id_ary = for_id[:6]  # Take first 6 elements
                    break
    
    # Pad with empty strings if needed (should have 6 elements)
    while len(id_ary) < 6:
        id_ary.append('')
    
    return id_ary[:6]  # Return exactly 6 elements


def build_header_record(args: Dict) -> str:
    """Build header record"""
    header_rec_ary = []
    header_rec_ary.append('1')
    header_rec_ary.append(args['fileType'])
    header_rec_ary.append('03' if args['test'] == 'Y' else '01')
    header_rec_ary.append('FTF')
    
    return '|'.join(header_rec_ary)


def build_trailer_record(args: Dict, file_stat=None) -> str:
    """Build trailer record"""
    if file_stat is None:
        file_epoch = int(time.time())
    else:
        file_epoch = int(file_stat.st_mtime)
    
    file_create_date = datetime.now().strftime('%Y%m%d')
    file_create_time = datetime.fromtimestamp(file_epoch).strftime('%H%M%S')
    file_ms = datetime.fromtimestamp(file_epoch).microsecond // 1000
    
    if 'recordCt' not in args:
        raise ValueError("Record Count argument is undefined")
    if 'acctHash' not in args:
        raise ValueError("Account Hash argument is undefined")
    
    file_acct_hash = args['acctHash']
    file_record_ct = args['recordCt']
    file_add_ct = args.get('added', 0)
    file_change_ct = args.get('changed', 0)
    file_delete_ct = args.get('deleted', 0)
    
    trailer_ary = [
        '9',
        args['fileType'],
        '03' if args['test'] == 'Y' else '01',
        'FTF'
    ]
    
    cde_vals = [
        'CDE0083', 'CDE0084', 'CDE0110', 'CDE0111', 'CDE0120',
        'CDE0121', 'CDE0123', 'CDE0133', 'CDE0139', 'CDE0151',
        'CDE0165', 'CDE0418', 'CDE0419', 'CDE0429', 'CDE0430',
        'CDE0467', 'CDE0674', 'CDE0676', 'CDE0811'
    ]
    
    trailer_vals = [
        file_create_date,
        f"{file_create_time}{file_ms}",
        file_acct_hash,
        file_add_ct,
        file_change_ct,
        file_delete_ct,
        '',
        file_record_ct,
        'ZOE',
        '',  # FI bank id
        '',  # FI region
        '',  # xfer date
        '',  # xfer time
        '',  # process end date
        '',  # process end time
        file_epoch,
        '',  # source file name
        'A',
        ''   # client id
    ]
    
    # Build CDE value pairs
    cde_pairs = [f"{cde_vals[i]}:{trailer_vals[i]}" for i in range(len(cde_vals))]
    trailer_ary.extend(cde_pairs)
    
    return '|'.join(str(val) for val in trailer_ary)


def get_zoe_file_hash(file_path: str) -> tuple:
    """Get hash of ZOE file records"""
    hash_zoe = {}
    acct_hash = 0
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('CDE') and '|' in line:
                    parts = line.split('|')
                    if len(parts) > 6:  # Should have at least record type, action, etc.
                        # Use account number as key (assuming it's in a specific position)
                        key = parts[6] if len(parts) > 6 else line
                        # Store the record without the first 5 fields (record metadata)
                        record_data = '|'.join(parts[5:]) if len(parts) > 5 else line
                        hash_zoe[key] = record_data
                        
                        # Add to account hash if account number is numeric
                        if len(parts) > 6 and parts[6].isdigit():
                            acct_hash += int(parts[6])
    except FileNotFoundError:
        print(f"File not found: {file_path}")
    except Exception as e:
        print(f"Error reading file {file_path}: {e}")
    
    return hash_zoe, acct_hash


def p2p_db_connect_func(args: dict, state: dict = None):
    """Connects to a SQL Server P2P database"""
    if state is None:
        state = {}
    
    p2p_server = args.get("p2pServer")
    p2p_schema = args.get("p2pSchema")
    p2p_driver = args.get("p2pDriverName", "SQL Server")

    dsn = (
        f"DRIVER={{{p2p_driver}}};"
        f"SERVER={p2p_server};"
        f"DATABASE={p2p_schema};"
        f"Trusted_Connection=yes;"
    )
    print("DSN--> ", dsn)

    try:
        dbh = pyodbc.connect(dsn)
        print("P2P DB Connected")
        return dbh
    except Exception as e:
        print(f"Failed to connect to P2P DB: {e}")
        return None


def dna_db_connect_func(args: dict, apwx: Apwx, state: dict = None):
    """Connects to DNA Oracle DB using AppWorx context"""
    if state is None:
        state = {}

    try:
        dbh = apwx.db_connect(autocommit=False)
        print("[DNA DB CONNECTED]")
        return dbh
    except Exception as e:
        print(f"Failed to connect to DNA DB: {e}")
        return None


def execute_sql_select(conn, sql: str) -> List[Dict]:
    """Executes a SQL SELECT and returns the result as a list of dicts"""
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            cols = [desc[0] for desc in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]
    except Exception as e:
        print(f"SQL execution error: {e}")
        return []


def build_cde_record() -> str:
    """Build CDE header record"""
    return '|'.join([
        "CDE0380", "CDE0377", "CDE0276", "CDE0157", "CDE0557", "CDE0014", "CDE0011", "CDE1023", "CDE0019", "CDE1024",
        "CDE1025", "CDE0023", "CDE0029", "CDE0032", "CDE0033", "CDE0036", "CDE0055", "CDE0056", "CDE0077", "CDE0100",
        "CDE1026", "CDE0141", "CDE0145", "CDE0166", "CDE0175", "CDE0182", "CDE0192", "CDE0199", "CDE0206", "CDE0215",
        "CDE0216", "CDE0219", "CDE0222", "CDE0227", "CDE0233", "CDE0277", "CDE1027", "CDE0238", "CDE0283", "CDE0284",
        "CDE0290", "CDE0299", "CDE0309", "CDE0319", "CDE0320", "CDE0321", "CDE0322", "CDE0323", "CDE0324", "CDE0334",
        "CDE0345", "CDE0354", "CDE0408", "CDE0409", "CDE0802", "CDE1275", "CDE1271", "CDE1272", "CDE1273", "CDE1274",
        "CDE0010"
    ])


def initialize(apwx: Apwx) -> ScriptData:
    """Initializes database connection, loads YAML config"""
    dbh = apwx.db_connect(autocommit=False)
    config = get_config(apwx)
    return ScriptData(apwx=apwx, dbh=dbh, config=config)


def get_config(apwx: Apwx) -> Any:
    """Load configuration from YAML file"""
    with open(apwx.args.CONFIG_FILE_PATH, "r") as f:
        return yaml.safe_load(f)


def get_apwx() -> Apwx:
    """Get AppWorx instance"""
    return Apwx(["OSIUPDATE", "OSIUPDATE_PW"])


def parse_args(apwx: Apwx) -> Apwx:
    """Parse command line arguments"""
    parser = apwx.parser
    parser.add_arg(AppWorxEnum.TNS_SERVICE_NAME, type=str, required=True)
    parser.add_arg(AppWorxEnum.CONFIG_FILE_PATH, type=r"(.yml|.yaml)$", required=True)
    parser.add_arg(AppWorxEnum.OUTPUT_FILE_NAME, type=str, required=True)
    parser.add_arg(AppWorxEnum.OUTPUT_FILE_PATH, type=parser.dir_validator, required=True)
    parser.add_arg(AppWorxEnum.TEST_YN, choices=["Y", "N"], default="N", required=False)
    parser.add_arg(AppWorxEnum.DEBUG_YN, choices=["Y", "N"], default="N", required=False)
    parser.add_arg(AppWorxEnum.MAX_THREADS, type=str, required=True)
    parser.add_arg(AppWorxEnum.MODE, type=str, required=True)
    parser.add_arg(AppWorxEnum.P2P_SERVER, type=str, required=True)
    parser.add_arg(AppWorxEnum.P2P_SCHEMA, type=str, required=True)
    parser.add_arg(AppWorxEnum.P2P_DRIVERNAME, type=str, required=True)
    parser.add_arg(AppWorxEnum.HOST, type=str, required=True)
    parser.add_arg(AppWorxEnum.SID, type=str, required=True)
    parser.add_arg(AppWorxEnum.RPT_ONLY, choices=["Y", "N"], default="N", required=False)
    
    # Add delta mode specific arguments
    parser.add_arg("OLD_ZOE_FILE", type=str, required=False)
    parser.add_arg("NEW_ZOE_FILE", type=str, required=False)
    
    apwx.parse_args()
    return apwx


if __name__ == "__main__":
    print(f"p2pZoeExtract.py\nVersion: {version}")
    print(f"Job started at {datetime.now()}")
    
    JobTime().print_start()
    run(parse_args(get_apwx()), time.time())
    JobTime().print_end()
    
    print(f"Job finished at {datetime.now()}")