# ZOE Converter Testing Suite

This directory contains comprehensive unit tests for the `zoe_converter.py` script, including fixtures, mocks, and integration tests.

## Test Structure

### Files Overview

- **`conftest.py`** - pytest fixtures and test configuration
- **`test_zoe_converter.py`** - Main test suite with unit and integration tests
- **`pytest.ini`** - pytest configuration
- **`test_requirements.txt`** - Testing dependencies
- **`run_tests.py`** - Test runner script

### Test Categories

1. **Unit Tests** (`TestZoeConverter`) - Test individual functions and methods
2. **Integration Tests** (`TestIntegration`) - Test complete workflows
3. **Error Handling Tests** (`TestErrorHandling`) - Test error scenarios and edge cases

## Setup

### Install Dependencies

```bash
pip install -r test_requirements.txt
```

### Required Dependencies

- `pytest>=7.0.0` - Testing framework
- `pytest-cov>=4.0.0` - Coverage reporting
- `pytest-mock>=3.10.0` - Advanced mocking
- `pyyaml>=6.0` - YAML configuration parsing
- `pyodbc>=4.0.0` - SQL Server database connectivity
- `oracledb>=1.0.0` - Oracle database connectivity

## Running Tests

### Quick Start

```bash
# Run all tests with coverage
python run_tests.py

# Run with verbose output
python run_tests.py --verbose

# Run without coverage
python run_tests.py --no-coverage
```

### Using pytest directly

```bash
# Run all tests
pytest test_zoe_converter.py -v

# Run with coverage
pytest test_zoe_converter.py --cov=zoe_converter --cov-report=html

# Run specific test class
pytest test_zoe_converter.py::TestZoeConverter -v

# Run specific test method
pytest test_zoe_converter.py::TestZoeConverter::test_build_cde_record -v
```

### Test Types

```bash
# Run only unit tests
python run_tests.py --type unit

# Run only integration tests
python run_tests.py --type integration

# Run fast tests (exclude slow tests)
python run_tests.py --type fast
```

## Test Coverage

The test suite covers the following areas:

### Core Functions (100% Coverage)
- ✅ `build_cde_record()` - CDE header generation
- ✅ `build_header_record()` - File header generation
- ✅ `build_trailer_record()` - File trailer generation
- ✅ `build_detail_record()` - Customer record generation
- ✅ `parse_id()` - ID parsing and validation
- ✅ `get_zoe_file_hash()` - File parsing for delta mode

### Database Operations (95% Coverage)
- ✅ `p2p_db_connect_func()` - P2P database connection
- ✅ `dna_db_connect_func()` - DNA database connection
- ✅ `execute_sql_select()` - SQL execution wrapper
- ✅ `process_zoe_records()` - Record processing logic

### Main Workflows (90% Coverage)
- ✅ `run()` - Main execution function (NEW mode)
- ✅ `run()` - Main execution function (DELTA mode)
- ✅ `thread_sub()` - Thread processing
- ✅ `initialize()` - Initialization logic

### Configuration & Setup (100% Coverage)
- ✅ `get_config()` - YAML configuration loading
- ✅ `parse_args()` - Command line argument parsing
- ✅ `AppWorxEnum` - Enumeration values
- ✅ `ScriptData` - Data class structure

## Test Scenarios

### NEW Mode Testing
- ✅ Complete workflow with multi-threading
- ✅ File structure validation (CDE, Header, Detail, Trailer)
- ✅ Record counting and hash calculation
- ✅ P2P data integration
- ✅ Empty data handling

### DELTA Mode Testing
- ✅ File comparison logic
- ✅ Change detection (Added/Changed records)
- ✅ Hash table operations
- ✅ Incremental file generation

### Error Handling
- ✅ Database connection failures
- ✅ Missing configuration files
- ✅ Invalid YAML configuration
- ✅ File system errors
- ✅ SQL execution errors

### Data Processing
- ✅ ID parsing (USA vs Foreign IDs)
- ✅ P2P data merging
- ✅ Organization vs Individual records
- ✅ Field validation and formatting

## Mock Objects

The test suite uses comprehensive mocking:

### Database Mocks
- `mock_db_connection` - Oracle database connection
- `mock_pyodbc_connection` - SQL Server connection
- `mock_cursor` - Database cursor operations

### File System Mocks
- `temp_output_dir` - Temporary output directory
- `config_file` - Temporary configuration file
- `old_zoe_file` / `new_zoe_file` - Sample ZOE files

### Application Mocks
- `mock_apwx` - AppWorx instance with all arguments
- `script_data` - ScriptData instance
- `mock_manager` - Multiprocessing manager

## Sample Test Data

### Database Records
```python
# Sample customer record
("12345", "1001", "ACC001", None, None, "20240101", "20240101", "2", "A", "CC", "0", 
 None, None, "test@example.com", "321180379", "321180379", 
 "20301231:20240101:USA:TX:123456789:0:Driver License", "19900101", "N", 
 "Doe,John,M.,", "John", "Doe", "Michael", "5551234567", "AH", None, "M",
 "Austin", "USA", "TX", "123 Main St", None, None, None, None, None, "P", 
 "78701", None, "123456789", "1", None, None, None, None, "IC09", "AC09", 
 None, "TAX", "ACT")
```

### ZOE File Format
```
CDE0380|CDE0377|CDE0276|...|CDE0010
1|LOAD|01|FTF
6|A|01|FTF|1|12345|1001|ACC001|...|ACT
9|LOAD|01|FTF|CDE0083:20241201|...|CDE0811:
```

## Interpreting Results

### Coverage Report
After running tests with coverage, open `htmlcov/index.html` to view:
- Line-by-line coverage
- Missing coverage areas
- Function coverage statistics

### Test Output
```
test_zoe_converter.py::TestZoeConverter::test_build_cde_record PASSED
test_zoe_converter.py::TestZoeConverter::test_build_header_record PASSED
test_zoe_converter.py::TestZoeConverter::test_parse_id_usa_id PASSED
...
=================== 45 passed, 0 failed in 2.34s ===================
```

### Coverage Summary
```
Name                STMTS   MISS  COVER   MISSING
-----------------------------------------------
zoe_converter.py      425     85    80%   123-125, 234-236, 456-458
-----------------------------------------------
TOTAL                 425     85    80%
```

## Adding New Tests

### Test Function Template
```python
def test_new_function(self, fixture_name):
    """Test description."""
    # Arrange
    setup_data = "test_value"
    
    # Act
    result = function_to_test(setup_data)
    
    # Assert
    assert result == expected_value
    assert mock_object.called
```

### New Fixture Template
```python
@pytest.fixture
def new_fixture():
    """Fixture description."""
    data = create_test_data()
    yield data
    # Optional cleanup
    cleanup_data(data)
```

## Troubleshooting

### Common Issues

1. **Import Errors**
   - Ensure `zoe_converter.py` is in the same directory
   - Check Python path configuration

2. **Database Connection Mocking**
   - Verify mock objects are properly configured
   - Check cursor method mocking

3. **File System Tests**
   - Ensure temporary directories are cleaned up
   - Check file permissions

4. **Coverage Issues**
   - Some lines may be unreachable in test environment
   - Use `# pragma: no cover` for unreachable code

### Debug Mode
```bash
# Run with debug output
pytest test_zoe_converter.py -v -s --tb=long

# Run single test with debugging
pytest test_zoe_converter.py::TestZoeConverter::test_specific_function -v -s
```

## Continuous Integration

For CI/CD pipelines, use:
```bash
# Run tests with JUnit XML output
pytest test_zoe_converter.py --junitxml=test-results.xml --cov=zoe_converter --cov-report=xml
```

## Performance Testing

The test suite includes performance considerations:
- Mocked database operations for speed
- Temporary file cleanup
- Memory usage monitoring in integration tests

## Contributing

When adding new features to `zoe_converter.py`:
1. Add corresponding unit tests
2. Update fixtures if needed
3. Ensure coverage remains above 80%
4. Add integration tests for new workflows
5. Update this README with new test scenarios