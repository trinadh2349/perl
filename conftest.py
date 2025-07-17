import pytest
import tempfile
import os
import yaml
from unittest.mock import Mock, MagicMock, patch
from dataclasses import dataclass
from typing import Dict, Any, List
import time
from datetime import datetime

# Import the classes and functions we need to test
from zoe_converter import ScriptData, AppWorxEnum


@pytest.fixture
def mock_apwx():
    """Mock AppWorx instance with required attributes and methods."""
    mock_apwx = Mock()
    
    # Mock args with all required attributes
    mock_apwx.args = Mock()
    mock_apwx.args.TNS_SERVICE_NAME = "test_tns"
    mock_apwx.args.CONFIG_FILE_PATH = "/test/config.yaml"
    mock_apwx.args.OUTPUT_FILE_NAME = "test_output.zoe"
    mock_apwx.args.OUTPUT_FILE_PATH = "/test/output"
    mock_apwx.args.TEST_YN = "Y"
    mock_apwx.args.DEBUG_YN = "N"
    mock_apwx.args.MAX_THREADS = "2"
    mock_apwx.args.MODE = "NEW"
    mock_apwx.args.P2P_SERVER = "test_server"
    mock_apwx.args.P2P_SCHEMA = "test_schema"
    mock_apwx.args.RPT_ONLY = "N"
    mock_apwx.args.OLD_ZOE_FILE = "/test/old.zoe"
    mock_apwx.args.NEW_ZOE_FILE = "/test/new.zoe"
    
    # Mock database connection method
    mock_apwx.db_connect = Mock()
    mock_db_connection = Mock()
    mock_apwx.db_connect.return_value = mock_db_connection
    
    return mock_apwx


@pytest.fixture
def mock_db_connection():
    """Mock database connection with cursor functionality."""
    mock_conn = Mock()
    mock_cursor = Mock()
    
    # Mock cursor methods
    mock_cursor.execute = Mock()
    mock_cursor.fetchmany = Mock()
    mock_cursor.fetchall = Mock()
    mock_cursor.close = Mock()
    mock_cursor.description = [("col1",), ("col2",), ("col3",)]
    
    mock_conn.cursor.return_value = mock_cursor
    mock_conn.close = Mock()
    
    return mock_conn


@pytest.fixture
def sample_config():
    """Sample YAML configuration for testing."""
    return {
        "sql_qq": "WITH adr AS (SELECT * FROM addr)",
        "cardTaxRptForPers": "SELECT * FROM cardTaxRptForPers WHERE MOD(persnbr, %(max_thread)s) = %(thread_id)s",
        "cardOwnPers": "SELECT * FROM cardOwnPers WHERE MOD(persnbr, %(max_thread)s) = %(thread_id)s",
        "noCardTaxRptForPers": "SELECT * FROM noCardTaxRptForPers WHERE MOD(persnbr, %(max_thread)s) = %(thread_id)s",
        "noCardOwnPers": "SELECT * FROM noCardOwnPers WHERE MOD(persnbr, %(max_thread)s) = %(thread_id)s",
        "cardOwnPersOrg": "SELECT * FROM cardOwnPersOrg WHERE MOD(persnbr, %(max_thread)s) = %(thread_id)s",
        "org": "SELECT * FROM org WHERE MOD(orgnbr, %(max_thread)s) = %(thread_id)s",
        "p2pCustOrg": "SELECT * FROM p2pCustOrg"
    }


@pytest.fixture
def config_file(sample_config):
    """Create a temporary YAML config file for testing."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        yaml.dump(sample_config, f)
        config_path = f.name
    
    yield config_path
    
    # Cleanup
    os.unlink(config_path)


@pytest.fixture
def temp_output_dir():
    """Create a temporary directory for output files."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield temp_dir


@pytest.fixture
def sample_database_records():
    """Sample database records for testing."""
    return [
        # Sample record with all fields
        (
            "12345",  # extcardnbr
            "1001",   # persnbr
            "ACC001", # acctnbr
            None,     # MICR_Current
            None,     # MICR_Old
            "20240101",  # contractdate
            "20240101",  # dsa_contractdate
            "2",      # acctsegmentct
            "A",      # acctsegtyp
            "CC",     # accttyp
            "0",      # businessindicator
            None,     # businessname
            None,     # contributionsource
            "test@example.com",  # email
            "321180379",  # rtnbr
            "321180379",  # abanbr
            "20301231:20240101:USA:TX:123456789:0:Driver License",  # idrow
            "19900101",  # datebirth
            "N",      # deceased
            "Doe,John,M.,",  # name
            "John",   # firstname
            "Doe",    # lastname
            "Michael", # mdlname
            "5551234567",  # phone
            "AH",     # phone type
            None,     # intldialcd
            "M",      # gender
            "Austin", # cityname
            "USA",    # ctrycd
            "TX",     # statecd
            "123 Main St",  # address
            None, None, None, None, None,  # alt addresses
            "P",      # addrtyp
            "78701",  # zipcd
            None,     # zip suffix
            "123456789",  # taxid
            "1",      # taxidtyp
            None, None, None, None,  # suffix, prefix, businesscd, businessdba
            "IC09",   # individualclassification
            "AC09",   # acctclassification
            None,     # acctclosedate
            "TAX",    # querysource
            "ACT"     # curracctstatcd
        )
    ]


@pytest.fixture
def sample_p2p_records():
    """Sample P2P database records for testing."""
    return [
        {
            "persnbr": "1001",
            "CXCCustomerID": "cxc123",
            "OrgId": "org456",
            "registeredEmail": "p2p@example.com",
            "registeredPhone": "5559876543"
        }
    ]


@pytest.fixture
def sample_zoe_file_content():
    """Sample ZOE file content for testing delta mode."""
    return """CDE0380|CDE0377|CDE0276|CDE0157|CDE0557|CDE0014|CDE0011|CDE1023|CDE0019|CDE1024|CDE1025|CDE0023|CDE0029|CDE0032|CDE0033|CDE0036|CDE0055|CDE0056|CDE0077|CDE0100|CDE1026|CDE0141|CDE0145|CDE0166|CDE0175|CDE0182|CDE0192|CDE0199|CDE0206|CDE0215|CDE0216|CDE0219|CDE0222|CDE0227|CDE0233|CDE0277|CDE1027|CDE0238|CDE0283|CDE0284|CDE0290|CDE0299|CDE0309|CDE0319|CDE0320|CDE0321|CDE0322|CDE0323|CDE0324|CDE0334|CDE0345|CDE0354|CDE0408|CDE0409|CDE0802|CDE1275|CDE1271|CDE1272|CDE1273|CDE1274|CDE0010
1|LOAD|01|FTF
6|A|01|FTF|1|12345|1001|ACC001||||20240101|20240101|2|A|CC|0||test@example.com|321180379|321180379|20301231:20240101:USA:TX:123456789:0:Driver License|19900101|N|Doe,John,M.,|John|Doe|Michael|5551234567|AH||M|Austin|USA|TX|123 Main St|||||P|78701||123456789|1||||IC09|AC09||TAX|ACT
9|LOAD|01|FTF|CDE0083:20241201|CDE0084:143022000|CDE0110:1001|CDE0111:1|CDE0120:0|CDE0121:0|CDE0123:|CDE0133:3|CDE0139:ZOE|CDE0151:|CDE0165:|CDE0418:|CDE0419:|CDE0429:|CDE0430:|CDE0467:1733071822|CDE0674:|CDE0676:A|CDE0811:"""


@pytest.fixture
def old_zoe_file(sample_zoe_file_content):
    """Create a temporary old ZOE file for delta testing."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.zoe', delete=False) as f:
        f.write(sample_zoe_file_content)
        old_file_path = f.name
    
    yield old_file_path
    
    # Cleanup
    os.unlink(old_file_path)


@pytest.fixture
def new_zoe_file():
    """Create a temporary new ZOE file for delta testing with modified content."""
    modified_content = """CDE0380|CDE0377|CDE0276|CDE0157|CDE0557|CDE0014|CDE0011|CDE1023|CDE0019|CDE1024|CDE1025|CDE0023|CDE0029|CDE0032|CDE0033|CDE0036|CDE0055|CDE0056|CDE0077|CDE0100|CDE1026|CDE0141|CDE0145|CDE0166|CDE0175|CDE0182|CDE0192|CDE0199|CDE0206|CDE0215|CDE0216|CDE0219|CDE0222|CDE0227|CDE0233|CDE0277|CDE1027|CDE0238|CDE0283|CDE0284|CDE0290|CDE0299|CDE0309|CDE0319|CDE0320|CDE0321|CDE0322|CDE0323|CDE0324|CDE0334|CDE0345|CDE0354|CDE0408|CDE0409|CDE0802|CDE1275|CDE1271|CDE1272|CDE1273|CDE1274|CDE0010
1|LOAD|01|FTF
6|A|01|FTF|1|12345|1001|ACC001||||20240101|20240101|2|A|CC|0||modified@example.com|321180379|321180379|20301231:20240101:USA:TX:123456789:0:Driver License|19900101|N|Doe,John,M.,|John|Doe|Michael|5551234567|AH||M|Austin|USA|TX|123 Main St|||||P|78701||123456789|1||||IC09|AC09||TAX|ACT
6|A|01|FTF|2|67890|1002|ACC002||||20240102|20240102|1|A|CS|0||new@example.com|321180379|321180379||19950615|N|Smith,Jane,,|Jane|Smith||5559876543|AH||F|Dallas|USA|TX|456 Oak Ave|||||P|75201||987654321|1||||IC09|AC09||TAX|ACT
9|LOAD|01|FTF|CDE0083:20241201|CDE0084:143022000|CDE0110:2003|CDE0111:2|CDE0120:0|CDE0121:0|CDE0123:|CDE0133:4|CDE0139:ZOE|CDE0151:|CDE0165:|CDE0418:|CDE0419:|CDE0429:|CDE0430:|CDE0467:1733071822|CDE0674:|CDE0676:A|CDE0811:"""
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.zoe', delete=False) as f:
        f.write(modified_content)
        new_file_path = f.name
    
    yield new_file_path
    
    # Cleanup
    os.unlink(new_file_path)


@pytest.fixture
def mock_file_stat():
    """Mock file stat object for testing."""
    mock_stat = Mock()
    mock_stat.st_mtime = time.time()
    return mock_stat


@pytest.fixture
def mock_manager():
    """Mock multiprocessing Manager for shared data structures."""
    mock_manager = Mock()
    mock_list = []
    mock_manager.list.return_value = mock_list
    return mock_manager


@pytest.fixture
def script_data(mock_apwx, mock_db_connection, sample_config):
    """Create a ScriptData instance for testing."""
    return ScriptData(
        apwx=mock_apwx,
        dbh=mock_db_connection,
        config=sample_config
    )


@pytest.fixture
def mock_pyodbc_connection():
    """Mock pyodbc connection for P2P database testing."""
    mock_conn = Mock()
    mock_cursor = Mock()
    
    mock_cursor.execute = Mock()
    mock_cursor.fetchmany = Mock()
    mock_cursor.fetchall = Mock()
    mock_cursor.close = Mock()
    mock_cursor.description = [("persnbr",), ("CXCCustomerID",), ("registeredEmail",)]
    
    mock_conn.cursor.return_value = mock_cursor
    mock_conn.close = Mock()
    
    return mock_conn


@pytest.fixture
def mock_time():
    """Mock time for consistent testing."""
    return 1733071822.0  # Fixed timestamp


@pytest.fixture
def mock_datetime():
    """Mock datetime for consistent testing."""
    with patch('zoe_converter.datetime') as mock_dt:
        mock_dt.now.return_value = datetime(2024, 12, 1, 14, 30, 22)
        mock_dt.fromtimestamp.return_value = datetime(2024, 12, 1, 14, 30, 22)
        yield mock_dt


@pytest.fixture(autouse=True)
def setup_test_environment(monkeypatch):
    """Set up test environment with necessary patches."""
    # Mock os.stat to return consistent file stats
    def mock_stat(path):
        mock_stat_obj = Mock()
        mock_stat_obj.st_mtime = 1733071822.0
        return mock_stat_obj
    
    monkeypatch.setattr("os.stat", mock_stat)
    
    # Mock time.time for consistent timestamps
    monkeypatch.setattr("time.time", lambda: 1733071822.0)
    monkeypatch.setattr("time.ctime", lambda x: "Sat Dec  1 14:30:22 2024")


@pytest.fixture
def sample_id_record():
    """Sample ID record string for testing parse_id function."""
    return "20301231:20240101:USA:TX:123456789:0:Driver License|20281231:20230101:USA:CA:987654321:4:State ID"


@pytest.fixture
def sample_build_detail_record_data():
    """Sample data for testing build_detail_record function."""
    return {
        "record_ary": [
            "12345", "1001", "ACC001", None, None, "20240101", "20240101", "2", "A", "CC", "0", None, None,
            "test@example.com", "321180379", "321180379", "20301231:20240101:USA:TX:123456789:0:Driver License",
            "19900101", "N", "Doe,John,M.,", "John", "Doe", "Michael", "5551234567", "AH", None, "M",
            "Austin", "USA", "TX", "123 Main St", None, None, None, None, None, "P", "78701", None,
            "123456789", "1", None, None, None, None, "IC09", "AC09", None, "TAX", "ACT"
        ],
        "p2p_cust": {
            "1001": {
                "CXCCustomerID": "cxc123",
                "registeredEmail": "p2p@example.com",
                "registeredPhone": "5559876543"
            }
        },
        "is_org": False
    }


@pytest.fixture
def sample_header_args():
    """Sample arguments for header record building."""
    return {
        "test": "Y",
        "fileType": "LOAD"
    }


@pytest.fixture
def sample_trailer_args():
    """Sample arguments for trailer record building."""
    return {
        "recordCt": 100,
        "added": 95,
        "changed": 5,
        "deleted": 0,
        "test": "Y",
        "fileType": "LOAD",
        "acctHash": 123456789
    }