# ZOE Data Extractor (Python)

A Python-based ZOE (Zero Operations Environment) data extraction tool that processes financial account data from DNA and P2P databases to generate formatted output files for downstream systems.

## Overview

This application extracts account and customer data from multiple database sources (Oracle DNA and SQL Server P2P) and generates pipe-delimited files in the ZOE format. It supports both full extraction (NEW mode) and incremental updates (DELTA mode) with multi-threaded processing for optimal performance.

## Features

- **Multi-threaded Processing**: Configurable thread count for parallel database queries
- **Dual Database Support**: Connects to both Oracle (DNA) and SQL Server (P2P) databases
- **Multiple Processing Modes**: NEW (full extract) and DELTA (incremental updates)
- **Comprehensive Record Types**: Supports 6 different query types for various account scenarios
- **P2P Integration**: Merges customer data from P2P system with DNA account data
- **Robust Error Handling**: Comprehensive exception handling and logging
- **Flexible Configuration**: YAML-based configuration for SQL queries

## Prerequisites

### Python Version
- Python 3.8 or higher

### Required Dependencies
```bash
pip install oracledb pyodbc pyyaml
```

### Database Access
- Oracle database connection via TNS service name
- SQL Server database connection (P2P system)
- AppWorx framework for credential management

## Installation

1. **Clone or download the project files:**
   ```bash
   # Ensure you have these files:
   # - zoe_converter.py
   # - config.yaml
   ```

2. **Install Python dependencies:**
   ```bash
   pip install oracledb pyodbc pyyaml
   ```

3. **Configure database connections:**
   - Ensure TNS service name is configured for Oracle
   - Verify SQL Server connection parameters
   - Set up AppWorx credentials (OSIUPDATE, OSIUPDATE_PW)

## Configuration

### config.yaml

The configuration file contains SQL queries for different record types:

```yaml
sql_qq: |
  # Common table expressions used by all queries
  WITH adr AS (...), ash AS (...)

cardTaxRptForPers: |
  # SQL for card holders with tax reporting

cardOwnPers: |
  # SQL for card owners (non-tax reporting)

# ... additional query configurations
```

### Required Parameters

| Parameter | Description | Example |
|-----------|-------------|---------|
| `TNS_SERVICE_NAME` | Oracle TNS service name | `DNATST4` |
| `CONFIG_FILE_PATH` | Path to YAML config file | `config.yaml` |
| `OUTPUT_FILE_NAME` | Output filename | `AOEP2P01.FTF` |
| `OUTPUT_FILE_PATH` | Output directory path | `/path/to/output` |
| `MAX_THREADS` | Number of processing threads | `8` |
| `MODE` | Processing mode | `NEW` or `DELTA` |
| `P2P_SERVER` | SQL Server instance | `SERVER,PORT` |
| `P2P_SCHEMA` | SQL Server database name | `P2P` |
| `P2P_DRIVERNAME` | ODBC driver name | `SQL Server` |

### Optional Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `TEST_YN` | Test mode flag | `N` |
| `DEBUG_YN` | Debug mode flag | `N` |
| `RPT_ONLY` | Report only mode | `N` |
| `OLD_ZOE_FILE` | Previous file for DELTA mode | (required for DELTA) |
| `NEW_ZOE_FILE` | New file for DELTA mode | (required for DELTA) |

## Usage

### Basic Usage (NEW Mode)

```bash
python zoe_converter.py \
  --TNS_SERVICE_NAME=DNATST4 \
  --CONFIG_FILE_PATH=config.yaml \
  --OUTPUT_FILE_NAME=AOEP2P01.FTF \
  --OUTPUT_FILE_PATH=/path/to/output \
  --MAX_THREADS=4 \
  --MODE=NEW \
  --P2P_SERVER=P2PPRODLS,58318 \
  --P2P_SCHEMA=P2P \
  --P2P_DRIVERNAME="SQL Server"
```

### Delta Mode Usage

```bash
python zoe_converter.py \
  --TNS_SERVICE_NAME=DNATST4 \
  --CONFIG_FILE_PATH=config.yaml \
  --OUTPUT_FILE_NAME=AOEP2P01.FTF \
  --OUTPUT_FILE_PATH=/path/to/output \
  --MAX_THREADS=4 \
  --MODE=DELTA \
  --P2P_SERVER=P2PPRODLS,58318 \
  --P2P_SCHEMA=P2P \
  --P2P_DRIVERNAME="SQL Server" \
  --OLD_ZOE_FILE=/path/to/previous_file.FTF \
  --NEW_ZOE_FILE=/path/to/current_file.FTF
```

### Test Mode

```bash
python zoe_converter.py \
  --TNS_SERVICE_NAME=DNATST4 \
  --CONFIG_FILE_PATH=config.yaml \
  --OUTPUT_FILE_NAME=AOEP2P01.FTF \
  --OUTPUT_FILE_PATH=/path/to/output \
  --MAX_THREADS=4 \
  --MODE=NEW \
  --TEST_YN=Y \
  --P2P_SERVER=P2PPRODLS,58318 \
  --P2P_SCHEMA=P2P \
  --P2P_DRIVERNAME="SQL Server"
```

## Output Format

### File Structure

The generated file contains three types of records:

1. **CDE Header Record**: Field definitions (line 1)
2. **Header Record**: File metadata (line 2)
3. **Detail Records**: Account/customer data (multiple lines)
4. **Trailer Record**: File summary and counts (last line)

### Record Format

Each detail record contains 61 pipe-delimited fields:

```
6|A|01|FTF|1|cardnbr|persnbr|cxcCustomerId|acctnbr|...|curracctstatcd
```

**Field Breakdown:**
- **Record Type**: Always "6" for detail records
- **Action**: "A" (Add), "C" (Change), "D" (Delete)
- **Environment**: "01" (Production), "03" (Test)
- **Institution**: "FTF"
- **Sequence**: Sequential record number
- **Data Fields**: 56 fields of account/customer data

## Processing Modes

### NEW Mode
- Performs full data extraction
- Processes all qualifying records
- Generates complete output file
- All records marked as "Add" actions

### DELTA Mode
- Compares two ZOE files
- Identifies changes between files
- Generates incremental update file
- Records marked as "Add" or "Change" actions

## Record Types Processed

1. **cardTaxRptForPers**: Card holders with tax reporting responsibilities
2. **cardOwnPers**: Card owners (non-tax reporting persons)
3. **noCardTaxRptForPers**: Tax reporting persons without cards
4. **noCardOwnPers**: Account owners without cards
5. **cardOwnPersOrg**: Card owners associated with organizations
6. **org**: Organization records

## Multi-threading

The application uses configurable multi-threading to improve performance:

- Each thread processes a subset of data based on `MOD(person_number, max_threads)`
- Threads share results via multiprocessing.Manager()
- Connection pooling prevents database resource conflicts
- Recommended thread count: 4-8 (adjust based on database capacity)

## Error Handling

### Common Issues

**Database Connection Errors:**
```
Failed to connect to DNA DB: ORA-12154: TNS:could not resolve the connect identifier
```
- Verify TNS service name configuration
- Check Oracle client installation

**Oracle Session Limit Errors:**
```
ORA-02391: exceeded simultaneous SESSIONS_PER_USER limit
```
- **Reduce MAX_THREADS parameter** (try 4 or fewer)
- Check Oracle database parameter: `SESSIONS_PER_USER`
- Each thread creates 1 Oracle connection
- Contact DBA to increase limit if needed

**SQL Server Connection Errors:**
```
Failed to connect to P2P DB: [Microsoft][ODBC Driver Manager] Data source name not found
```
- Verify ODBC driver installation
- Check SQL Server connection parameters

**Permission Errors:**
```
Could not open file for output at /path/to/output/AOEP2P01.FTF
```
- Verify output directory exists and is writable
- Check file system permissions

### Logging

The application provides detailed console output:
- Thread start/completion messages
- Record count summaries
- Error details with stack traces
- Performance timing information

## Performance Tuning

### Thread Count Optimization
- **Start with 4 threads for testing**
- **Check Oracle SESSIONS_PER_USER limit**: Each thread uses 1 Oracle connection
- Increase gradually based on database performance
- Monitor database connection limits
- **Typical production setting: 4-6 threads** (depending on DB limits)
- **If you get ORA-02391 errors**: Reduce thread count

### Memory Management
- Application uses streaming processing for large datasets
- Records processed in batches of 1000
- Memory usage scales with thread count and batch size

### Database Optimization
- Ensure proper indexing on MOD operations
- Monitor database connection pool usage
- Consider database-specific tuning parameters

## File Examples

### Sample Output File Structure
```
CDE0380|CDE0377|CDE0276|...|CDE0010
1|LOAD|01|FTF
6|A|01|FTF|1||123456|ABC123|987654|...|ACT
6|A|01|FTF|2||123457|ABC124|987655|...|ACT
...
9|LOAD|01|FTF|CDE0083:20231201|CDE0084:143022500|...|CDE0811:
```

### Configuration Example
```yaml
sql_qq: |
  WITH adr AS (
    SELECT DISTINCT persnbr, address, cityname, statecd
    FROM address_view
  )

cardTaxRptForPers: |
  SELECT '' extcardnbr, a.taxrptforpersnbr persnbr, a.acctnbr
  FROM acct a
  WHERE MOD(a.taxrptforpersnbr, %(max_thread)s) = %(thread_id)s
```

## Migration from Perl

This Python version is a direct replacement for the original Perl implementation with these improvements:

- Enhanced error handling and logging
- Modern Python coding standards
- Improved multi-threading implementation
- Better configuration management
- Comprehensive documentation

All output formats and business logic remain identical to ensure compatibility with downstream systems.

## Support

For issues or questions:

1. Check the error logs for specific error messages
2. Verify database connectivity independently
3. Test with reduced thread count for debugging
4. Validate configuration file syntax

## Version History

- **v1.00**: Initial Python conversion from Perl
  - Complete feature parity with original Perl implementation
  - Enhanced error handling and documentation
  - Improved multi-threading performance