import pytest
import os
import tempfile
import yaml
from unittest.mock import Mock, patch, MagicMock, call
from multiprocessing import Manager
import time
from datetime import datetime

# Import functions to test
from zoe_converter import (
    run, initialize, get_config, parse_args, get_apwx,
    build_detail_record, parse_id, build_header_record, build_trailer_record,
    build_cde_record, get_zoe_file_hash, p2p_db_connect_func, dna_db_connect_func,
    execute_sql_select, process_zoe_records, thread_sub,
    ScriptData, AppWorxEnum
)


class TestZoeConverter:
    """Test suite for zoe_converter.py"""

    def test_app_worx_enum_values(self):
        """Test AppWorxEnum contains all required values."""
        expected_values = [
            'TNS_SERVICE_NAME', 'CONFIG_FILE_PATH', 'OUTPUT_FILE_NAME',
            'OUTPUT_FILE_PATH', 'TEST_YN', 'DEBUG_YN', 'MAX_THREADS',
            'MODE', 'P2P_SERVER', 'P2P_SCHEMA', 'RPT_ONLY',
            'OLD_ZOE_FILE', 'NEW_ZOE_FILE'
        ]
        
        for value in expected_values:
            assert hasattr(AppWorxEnum, value)
            assert str(getattr(AppWorxEnum, value)) == value

    def test_script_data_dataclass(self, mock_apwx, mock_db_connection, sample_config):
        """Test ScriptData dataclass creation."""
        script_data = ScriptData(
            apwx=mock_apwx,
            dbh=mock_db_connection,
            config=sample_config
        )
        
        assert script_data.apwx == mock_apwx
        assert script_data.dbh == mock_db_connection
        assert script_data.config == sample_config

    def test_get_config(self, config_file, sample_config):
        """Test loading configuration from YAML file."""
        mock_apwx = Mock()
        mock_apwx.args.CONFIG_FILE_PATH = config_file
        
        config = get_config(mock_apwx)
        
        assert config == sample_config
        assert 'sql_qq' in config
        assert 'cardTaxRptForPers' in config

    def test_initialize(self, mock_apwx, config_file, sample_config):
        """Test initialization function."""
        mock_apwx.args.CONFIG_FILE_PATH = config_file
        
        with patch('zoe_converter.get_config', return_value=sample_config):
            script_data = initialize(mock_apwx)
            
            assert isinstance(script_data, ScriptData)
            assert script_data.apwx == mock_apwx
            assert script_data.config == sample_config
            mock_apwx.db_connect.assert_called_once_with(autocommit=False)

    def test_build_cde_record(self):
        """Test CDE record building."""
        cde_record = build_cde_record()
        
        assert cde_record.startswith("CDE0380|CDE0377|CDE0276")
        assert cde_record.endswith("CDE0010")
        assert cde_record.count("|") == 60  # Should have 61 fields separated by 60 pipes

    def test_build_header_record(self, sample_header_args):
        """Test header record building."""
        header = build_header_record(sample_header_args)
        
        expected = "1|LOAD|03|FTF"
        assert header == expected

    def test_build_header_record_production(self):
        """Test header record building for production mode."""
        args = {"test": "N", "fileType": "UPDT"}
        header = build_header_record(args)
        
        expected = "1|UPDT|01|FTF"
        assert header == expected

    def test_build_trailer_record(self, sample_trailer_args, mock_file_stat):
        """Test trailer record building."""
        trailer = build_trailer_record(sample_trailer_args, mock_file_stat)
        
        assert trailer.startswith("9|LOAD|03|FTF")
        assert "CDE0083:20241201" in trailer
        assert "CDE0110:123456789" in trailer
        assert "CDE0111:95" in trailer  # added count
        assert "CDE0120:5" in trailer   # changed count
        assert "CDE0121:0" in trailer   # deleted count

    def test_build_trailer_record_missing_args(self):
        """Test trailer record building with missing required arguments."""
        args = {"test": "Y", "fileType": "LOAD"}
        
        with pytest.raises(ValueError, match="Record Count argument is undefined"):
            build_trailer_record(args)
        
        args["recordCt"] = 100
        with pytest.raises(ValueError, match="Account Hash argument is undefined"):
            build_trailer_record(args)

    def test_parse_id_usa_id(self):
        """Test parsing ID record with USA ID."""
        id_string = "20301231:20240101:USA:TX:123456789:0:Driver License"
        result = parse_id(id_string, is_org=False)
        
        expected = ["20301231", "20240101", "USA", "TX", "123456789", "0"]
        assert result == expected

    def test_parse_id_multiple_ids(self):
        """Test parsing ID record with multiple IDs, prioritizing USA."""
        id_string = "20281231:20230101:CAN:ON:987654321:4:Foreign ID|20301231:20240101:USA:TX:123456789:0:Driver License"
        result = parse_id(id_string, is_org=False)
        
        # Should prioritize USA ID
        expected = ["20301231", "20240101", "USA", "TX", "123456789", "0"]
        assert result == expected

    def test_parse_id_foreign_only(self):
        """Test parsing ID record with only foreign ID."""
        id_string = "20281231:20230101:CAN:ON:987654321:4:Foreign ID"
        result = parse_id(id_string, is_org=False)
        
        expected = ["20281231", "20230101", "CAN", "ON", "987654321", "4"]
        assert result == expected

    def test_parse_id_org_mode(self):
        """Test parsing ID record for organization (should return empty)."""
        id_string = "20301231:20240101:USA:TX:123456789:0:Driver License"
        result = parse_id(id_string, is_org=True)
        
        expected = ["", "", "", "", "", ""]
        assert result == expected

    def test_parse_id_empty_string(self):
        """Test parsing empty ID string."""
        result = parse_id("", is_org=False)
        
        expected = ["", "", "", "", "", ""]
        assert result == expected

    def test_build_detail_record_individual(self, sample_build_detail_record_data):
        """Test building detail record for individual customer."""
        data = sample_build_detail_record_data
        result = build_detail_record(data["record_ary"], data["p2p_cust"], data["is_org"])
        
        assert result.startswith("12345|1001|cxc123")  # Should use P2P customer ID
        assert "p2p@example.com" in result  # Should use P2P email
        assert "5559876543" in result  # Should use P2P phone
        assert result.endswith("ACT")

    def test_build_detail_record_organization(self):
        """Test building detail record for organization."""
        record_ary = ["12345", "2001", "ACC001"] + [""] * 47
        p2p_cust = {}
        
        result = build_detail_record(record_ary, p2p_cust, is_org=True)
        
        assert result.startswith("12345|2001|2001")  # Should use persnbr as customer ID for org
        assert result.count("|") > 50  # Should have proper field count

    def test_build_detail_record_short_record(self):
        """Test building detail record with insufficient fields."""
        record_ary = ["12345"]
        p2p_cust = {}
        
        result = build_detail_record(record_ary, p2p_cust, is_org=False)
        
        assert result == ""  # Should return empty string for insufficient data

    def test_get_zoe_file_hash(self, old_zoe_file):
        """Test parsing ZOE file into hash structure."""
        hash_zoe, acct_hash = get_zoe_file_hash(old_zoe_file)
        
        assert isinstance(hash_zoe, dict)
        assert isinstance(acct_hash, int)
        assert len(hash_zoe) > 0
        assert "ACC001" in hash_zoe  # Should contain account number as key

    def test_get_zoe_file_hash_nonexistent_file(self):
        """Test parsing non-existent ZOE file."""
        hash_zoe, acct_hash = get_zoe_file_hash("/nonexistent/file.zoe")
        
        assert hash_zoe == {}
        assert acct_hash == 0

    def test_execute_sql_select(self, mock_db_connection):
        """Test SQL execution and result formatting."""
        # Mock cursor results
        mock_cursor = mock_db_connection.cursor.return_value
        mock_cursor.fetchall.return_value = [
            ("value1", "value2", "value3"),
            ("value4", "value5", "value6")
        ]
        mock_cursor.description = [("col1",), ("col2",), ("col3",)]
        
        result = execute_sql_select(mock_db_connection, "SELECT * FROM test")
        
        expected = [
            {"col1": "value1", "col2": "value2", "col3": "value3"},
            {"col1": "value4", "col2": "value5", "col3": "value6"}
        ]
        
        assert result == expected
        mock_cursor.execute.assert_called_once_with("SELECT * FROM test")

    def test_execute_sql_select_exception(self, mock_db_connection):
        """Test SQL execution with exception handling."""
        mock_cursor = mock_db_connection.cursor.return_value
        mock_cursor.execute.side_effect = Exception("Database error")
        
        result = execute_sql_select(mock_db_connection, "SELECT * FROM test")
        
        assert result == []

    @patch('zoe_converter.pyodbc.connect')
    def test_p2p_db_connect_func_success(self, mock_connect):
        """Test successful P2P database connection."""
        mock_connection = Mock()
        mock_connect.return_value = mock_connection
        
        args = {
            "p2pServer": "test_server",
            "p2pSchema": "test_schema"
        }
        
        result = p2p_db_connect_func(args)
        
        assert result == mock_connection
        mock_connect.assert_called_once()

    @patch('zoe_converter.pyodbc.connect')
    def test_p2p_db_connect_func_failure(self, mock_connect):
        """Test P2P database connection failure."""
        mock_connect.side_effect = Exception("Connection failed")
        
        args = {
            "p2pServer": "test_server",
            "p2pSchema": "test_schema"
        }
        
        result = p2p_db_connect_func(args)
        
        assert result is None

    def test_dna_db_connect_func_success(self, mock_apwx):
        """Test successful DNA database connection."""
        mock_connection = Mock()
        mock_apwx.db_connect.return_value = mock_connection
        
        result = dna_db_connect_func({}, mock_apwx)
        
        assert result == mock_connection
        mock_apwx.db_connect.assert_called_once_with(autocommit=False)

    def test_dna_db_connect_func_failure(self, mock_apwx):
        """Test DNA database connection failure."""
        mock_apwx.db_connect.side_effect = Exception("Connection failed")
        
        result = dna_db_connect_func({}, mock_apwx)
        
        assert result is None

    @patch('zoe_converter.execute_sql_select')
    def test_process_zoe_records(self, mock_execute, mock_db_connection, mock_pyodbc_connection, script_data, mock_apwx):
        """Test processing ZOE records from database."""
        # Mock P2P customer data
        mock_execute.return_value = [
            {"persnbr": "1001", "CXCCustomerID": "cxc123", "registeredEmail": "test@example.com"}
        ]
        
        # Mock DNA database cursor
        mock_cursor = mock_db_connection.cursor.return_value
        mock_cursor.fetchmany.side_effect = [
            [("12345", "1001", "ACC001") + ("",) * 47],  # First batch
            []  # End of results
        ]
        
        zoe_data = []
        
        with patch('zoe_converter.build_detail_record', return_value="test_record"):
            process_zoe_records(
                mock_db_connection, mock_pyodbc_connection, script_data,
                max_thread=2, thread_id=0, zoe_data=zoe_data, apwx=mock_apwx
            )
        
        assert len(zoe_data) > 0
        mock_cursor.execute.assert_called()

    @patch('zoe_converter.process_zoe_records')
    @patch('zoe_converter.dna_db_connect_func')
    @patch('zoe_converter.p2p_db_connect_func')
    def test_thread_sub(self, mock_p2p_connect, mock_dna_connect, mock_process, script_data, mock_apwx):
        """Test thread subprocess functionality."""
        mock_p2p_conn = Mock()
        mock_dna_conn = Mock()
        mock_p2p_connect.return_value = mock_p2p_conn
        mock_dna_connect.return_value = mock_dna_conn
        
        zoe_data = []
        
        with patch('time.sleep'):  # Skip sleep in tests
            thread_sub(1, script_data, mock_apwx, 0, 2, zoe_data, mock_apwx)
        
        mock_p2p_connect.assert_called_once()
        mock_dna_connect.assert_called_once()
        mock_process.assert_called_once()
        mock_dna_conn.close.assert_called_once()

    @patch('zoe_converter.initialize')
    @patch('zoe_converter.threading.Thread')
    @patch('zoe_converter.Manager')
    def test_run_new_mode(self, mock_manager, mock_thread, mock_initialize, mock_apwx, temp_output_dir):
        """Test main run function in NEW mode."""
        # Setup
        mock_apwx.args.MODE = "NEW"
        mock_apwx.args.OUTPUT_FILE_PATH = temp_output_dir
        mock_apwx.args.OUTPUT_FILE_NAME = "test_output.zoe"
        mock_apwx.args.MAX_THREADS = "2"
        mock_apwx.args.TEST_YN = "Y"
        
        mock_script_data = Mock()
        mock_initialize.return_value = mock_script_data
        
        mock_manager_instance = Mock()
        mock_zoe_data = ["test_record1", "test_record2"]
        mock_manager_instance.list.return_value = mock_zoe_data
        mock_manager.return_value = mock_manager_instance
        
        mock_thread_instance = Mock()
        mock_thread.return_value = mock_thread_instance
        
        # Execute
        result = run(mock_apwx, time.time())
        
        # Verify
        assert result is True
        mock_initialize.assert_called_once_with(mock_apwx)
        assert mock_thread.call_count == 2  # Should create 2 threads
        mock_thread_instance.start.assert_called()
        mock_thread_instance.join.assert_called()
        
        # Check output file was created
        output_file = os.path.join(temp_output_dir, "test_output.zoe")
        assert os.path.exists(output_file)

    @patch('zoe_converter.initialize')
    @patch('zoe_converter.get_zoe_file_hash')
    def test_run_delta_mode(self, mock_get_hash, mock_initialize, mock_apwx, temp_output_dir, old_zoe_file, new_zoe_file):
        """Test main run function in DELTA mode."""
        # Setup
        mock_apwx.args.MODE = "DELTA"
        mock_apwx.args.OUTPUT_FILE_PATH = temp_output_dir
        mock_apwx.args.OUTPUT_FILE_NAME = "test_delta.zoe"
        mock_apwx.args.TEST_YN = "Y"
        mock_apwx.args.OLD_ZOE_FILE = old_zoe_file
        mock_apwx.args.NEW_ZOE_FILE = new_zoe_file
        
        mock_script_data = Mock()
        mock_initialize.return_value = mock_script_data
        
        # Mock hash data
        old_hash = {"ACC001": "old_data"}
        new_hash = {"ACC001": "new_data", "ACC002": "new_record"}
        mock_get_hash.side_effect = [(old_hash, 1001), (new_hash, 2003)]
        
        # Execute
        result = run(mock_apwx, time.time())
        
        # Verify
        assert result is True
        mock_initialize.assert_called_once_with(mock_apwx)
        assert mock_get_hash.call_count == 2
        
        # Check output file was created
        output_file = os.path.join(temp_output_dir, "test_delta.zoe")
        assert os.path.exists(output_file)

    def test_run_invalid_mode(self, mock_apwx):
        """Test run function with invalid mode."""
        mock_apwx.args.MODE = "INVALID"
        
        with pytest.raises(ValueError, match="Invalid MODE. Must be 'NEW' or 'DELTA'"):
            run(mock_apwx, time.time())

    @patch('zoe_converter.get_apwx')
    @patch('zoe_converter.parse_args')
    @patch('zoe_converter.run')
    @patch('zoe_converter.JobTime')
    def test_main_execution(self, mock_jobtime, mock_run, mock_parse_args, mock_get_apwx):
        """Test main execution flow."""
        mock_apwx = Mock()
        mock_get_apwx.return_value = mock_apwx
        mock_parse_args.return_value = mock_apwx
        mock_run.return_value = True
        
        mock_jobtime_instance = Mock()
        mock_jobtime.return_value = mock_jobtime_instance
        
        # Import and execute the main block
        with patch('zoe_converter.__name__', '__main__'):
            with patch('builtins.print'):  # Suppress print statements
                exec(open('zoe_converter.py').read())
        
        mock_get_apwx.assert_called_once()
        mock_parse_args.assert_called_once_with(mock_apwx)
        mock_run.assert_called_once()
        mock_jobtime_instance.print_start.assert_called_once()
        mock_jobtime_instance.print_end.assert_called_once()

    def test_parse_args_configuration(self):
        """Test argument parsing configuration."""
        mock_apwx = Mock()
        mock_parser = Mock()
        mock_apwx.parser = mock_parser
        
        result = parse_args(mock_apwx)
        
        # Verify all required arguments are added
        expected_args = [
            AppWorxEnum.TNS_SERVICE_NAME,
            AppWorxEnum.CONFIG_FILE_PATH,
            AppWorxEnum.OUTPUT_FILE_NAME,
            AppWorxEnum.OUTPUT_FILE_PATH,
            AppWorxEnum.TEST_YN,
            AppWorxEnum.DEBUG_YN,
            AppWorxEnum.MAX_THREADS,
            AppWorxEnum.MODE,
            AppWorxEnum.P2P_SERVER,
            AppWorxEnum.P2P_SCHEMA,
            AppWorxEnum.RPT_ONLY,
            AppWorxEnum.OLD_ZOE_FILE,
            AppWorxEnum.NEW_ZOE_FILE
        ]
        
        assert mock_parser.add_arg.call_count == len(expected_args)
        mock_apwx.parse_args.assert_called_once()
        assert result == mock_apwx


class TestIntegration:
    """Integration tests for complete workflows."""
    
    @patch('zoe_converter.threading.Thread')
    @patch('zoe_converter.Manager')
    @patch('zoe_converter.initialize')
    def test_new_mode_complete_workflow(self, mock_initialize, mock_manager, mock_thread, 
                                       mock_apwx, temp_output_dir, sample_config):
        """Test complete NEW mode workflow."""
        # Setup mocks
        mock_apwx.args.MODE = "NEW"
        mock_apwx.args.OUTPUT_FILE_PATH = temp_output_dir
        mock_apwx.args.OUTPUT_FILE_NAME = "integration_test.zoe"
        mock_apwx.args.MAX_THREADS = "1"
        mock_apwx.args.TEST_YN = "N"
        
        mock_script_data = Mock()
        mock_script_data.config = sample_config
        mock_initialize.return_value = mock_script_data
        
        # Mock manager and shared data
        mock_manager_instance = Mock()
        mock_zoe_data = [
            "12345|1001|ACC001||||20240101|20240101|2|A|CC|0||test@example.com|321180379|321180379||19900101|N|Doe,John,M.,|John|Doe|Michael|5551234567|AH||M|Austin|USA|TX|123 Main St|||||P|78701||123456789|1||||IC09|AC09||TAX|ACT"
        ]
        mock_manager_instance.list.return_value = mock_zoe_data
        mock_manager.return_value = mock_manager_instance
        
        # Mock thread
        mock_thread_instance = Mock()
        mock_thread.return_value = mock_thread_instance
        
        # Execute
        result = run(mock_apwx, 1733071822.0)
        
        # Verify
        assert result is True
        
        # Check output file structure
        output_file = os.path.join(temp_output_dir, "integration_test.zoe")
        assert os.path.exists(output_file)
        
        with open(output_file, 'r') as f:
            content = f.read()
            
        lines = content.strip().split('\n')
        assert len(lines) >= 3  # CDE, Header, Trailer minimum
        assert lines[0].startswith("CDE0380")  # CDE record
        assert lines[1] == "1|LOAD|01|FTF"  # Header record
        assert lines[-1].startswith("9|LOAD|01|FTF")  # Trailer record

    @patch('zoe_converter.initialize')
    @patch('zoe_converter.get_zoe_file_hash')
    def test_delta_mode_complete_workflow(self, mock_get_hash, mock_initialize, 
                                         mock_apwx, temp_output_dir, old_zoe_file, new_zoe_file):
        """Test complete DELTA mode workflow."""
        # Setup
        mock_apwx.args.MODE = "DELTA"
        mock_apwx.args.OUTPUT_FILE_PATH = temp_output_dir
        mock_apwx.args.OUTPUT_FILE_NAME = "delta_test.zoe"
        mock_apwx.args.TEST_YN = "N"
        mock_apwx.args.OLD_ZOE_FILE = old_zoe_file
        mock_apwx.args.NEW_ZOE_FILE = new_zoe_file
        
        mock_script_data = Mock()
        mock_initialize.return_value = mock_script_data
        
        # Setup hash data to simulate changes
        old_hash = {
            "ACC001": "12345|1001|ACC001||||20240101|20240101|2|A|CC|0||test@example.com|321180379|321180379||19900101|N|Doe,John,M.,|John|Doe|Michael|5551234567|AH||M|Austin|USA|TX|123 Main St|||||P|78701||123456789|1||||IC09|AC09||TAX|ACT"
        }
        new_hash = {
            "ACC001": "12345|1001|ACC001||||20240101|20240101|2|A|CC|0||modified@example.com|321180379|321180379||19900101|N|Doe,John,M.,|John|Doe|Michael|5551234567|AH||M|Austin|USA|TX|123 Main St|||||P|78701||123456789|1||||IC09|AC09||TAX|ACT",
            "ACC002": "67890|1002|ACC002||||20240102|20240102|1|A|CS|0||new@example.com|321180379|321180379||19950615|N|Smith,Jane,,|Jane|Smith||5559876543|AH||F|Dallas|USA|TX|456 Oak Ave|||||P|75201||987654321|1||||IC09|AC09||TAX|ACT"
        }
        
        mock_get_hash.side_effect = [(old_hash, 1001), (new_hash, 2003)]
        
        # Execute
        result = run(mock_apwx, 1733071822.0)
        
        # Verify
        assert result is True
        
        # Check output file
        output_file = os.path.join(temp_output_dir, "delta_test.zoe")
        assert os.path.exists(output_file)
        
        with open(output_file, 'r') as f:
            content = f.read()
        
        lines = content.strip().split('\n')
        assert len(lines) >= 3  # CDE, Header, Trailer minimum
        assert lines[0].startswith("CDE0380")  # CDE record
        assert lines[1] == "1|UPDT|01|FTF"  # Header record (UPDT for delta)
        assert lines[-1].startswith("9|UPDT|01|FTF")  # Trailer record
        
        # Should contain both changed (C) and added (A) records
        detail_lines = [line for line in lines if line.startswith("6|")]
        assert len(detail_lines) == 2  # One changed, one added
        assert any("6|C|01|FTF" in line for line in detail_lines)  # Changed record
        assert any("6|A|01|FTF" in line for line in detail_lines)  # Added record


class TestErrorHandling:
    """Test error handling and edge cases."""
    
    def test_missing_config_file(self, mock_apwx):
        """Test handling of missing configuration file."""
        mock_apwx.args.CONFIG_FILE_PATH = "/nonexistent/config.yaml"
        
        with pytest.raises(FileNotFoundError):
            get_config(mock_apwx)

    def test_invalid_yaml_config(self, mock_apwx):
        """Test handling of invalid YAML configuration."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write("invalid: yaml: content: [")
            invalid_config_path = f.name
        
        try:
            mock_apwx.args.CONFIG_FILE_PATH = invalid_config_path
            
            with pytest.raises(yaml.YAMLError):
                get_config(mock_apwx)
        finally:
            os.unlink(invalid_config_path)

    def test_output_directory_creation(self, mock_apwx):
        """Test output directory handling."""
        with tempfile.TemporaryDirectory() as temp_dir:
            nonexistent_subdir = os.path.join(temp_dir, "nonexistent", "subdir")
            mock_apwx.args.OUTPUT_FILE_PATH = nonexistent_subdir
            mock_apwx.args.OUTPUT_FILE_NAME = "test.zoe"
            mock_apwx.args.MODE = "NEW"
            mock_apwx.args.MAX_THREADS = "1"
            mock_apwx.args.TEST_YN = "Y"
            
            # This should handle the missing directory gracefully
            with patch('zoe_converter.initialize') as mock_init:
                mock_init.return_value = Mock()
                with patch('zoe_converter.threading.Thread'):
                    with patch('zoe_converter.Manager') as mock_manager:
                        mock_manager.return_value.list.return_value = []
                        
                        # Should not raise an exception for missing directory
                        # The open() call will create the directory path if needed
                        with pytest.raises(FileNotFoundError):
                            run(mock_apwx, time.time())

    def test_database_connection_failure_handling(self, mock_apwx):
        """Test handling of database connection failures."""
        mock_apwx.db_connect.side_effect = Exception("Database connection failed")
        
        with pytest.raises(Exception):
            initialize(mock_apwx)

    def test_empty_zoe_data_handling(self, mock_apwx, temp_output_dir):
        """Test handling when no ZOE data is found."""
        mock_apwx.args.MODE = "NEW"
        mock_apwx.args.OUTPUT_FILE_PATH = temp_output_dir
        mock_apwx.args.OUTPUT_FILE_NAME = "empty_test.zoe"
        mock_apwx.args.MAX_THREADS = "1"
        mock_apwx.args.TEST_YN = "Y"
        
        with patch('zoe_converter.initialize') as mock_init:
            mock_init.return_value = Mock()
            with patch('zoe_converter.threading.Thread'):
                with patch('zoe_converter.Manager') as mock_manager:
                    mock_manager.return_value.list.return_value = []  # Empty data
                    
                    result = run(mock_apwx, time.time())
                    
                    assert result is True
                    
                    # File should still be created with header and trailer
                    output_file = os.path.join(temp_output_dir, "empty_test.zoe")
                    assert os.path.exists(output_file)
                    
                    with open(output_file, 'r') as f:
                        content = f.read()
                    
                    lines = content.strip().split('\n')
                    assert len(lines) == 3  # CDE, Header, Trailer only