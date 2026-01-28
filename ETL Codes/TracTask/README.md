# Tractask ETL Pipeline - Complete Documentation

## Overview
This ETL (Extract, Transform, Load) pipeline synchronizes task tracking data from DynamoDB to PostgreSQL with intelligent update detection and workflow-based status mapping.

## Key Features

### 1. **Workflow-Based Status Mapping**
- ✅ Dynamically loads status definitions from `workflow.json`
- ✅ Each workflow can have different status meanings
- ✅ Automatically updates when workflow.json changes

### 2. **Comprehensive Data Updates**
The ETL detects and updates changes in:
- Status and Status ID
- Employee assignments
- Workflow ID (now dynamically tracked)
- Project names (including new segregations: Insyts-BDO, Insyts-Global, Nexus)
- Group names
- Priority levels
- Deadline dates and times
- Closed dates and times

### 3. **Smart Closed Date/Time Handling**
- Auto-populates closed date/time when status is "Closed", "Completed", or "Verified"
- If no closed date provided, uses reported date as fallback
- Handles NULL values properly for incomplete data

### 4. **All Record Types Included**
- ✅ Removed Type filter - ALL records are now processed
- Records are categorized by Type into groups (Bug, Issue, New Task, Request for Change, Others)

### 5. **Enhanced Project Name Mapping**
New project segregations:
```
Insyts → Split into:
  - Insyts-BDO
  - Insyts-Global
  - Insyts-Amadi

New Projects:
  - Nexus
  - LMS
  - UIUX
```

### 6. **NULL Handling**
- ✅ Empty strings converted to NULL in database
- ✅ Missing dates/times properly handled
- ✅ No data quality issues from empty values

## Project Structure

```
.
├── tractask_updated.py      # Main ETL script
├── workflow.json             # Workflow status definitions
├── diagnose_types.py         # Diagnostic tool for DynamoDB
└── README.md                 # This file
```

## Configuration

### Database Connections
```python
CONFIG = {
    # DynamoDB
    "aws_access_key_id": "YOUR_KEY",
    "aws_secret_access_key": "YOUR_SECRET",
    "region_name": "",
    "table_name": "",
    "tracking_client_id": "",
    
    # PostgreSQL
    "postgres_host": "your-host.rds.amazonaws.com",
    "postgres_port": 5432,
    "postgres_database": "",
    "postgres_user": "username",
    "postgres_password": "password",
    "postgres_table": "",
    
    # Workflow Configuration
    "workflow_json_path": "workflow.json"
}
```

## ETL Process Flow

### Step 1: Load Workflows
```
📋 Loading workflow configurations...
✅ Loaded 92 workflows
```
Reads workflow.json and builds status mapping dictionaries for each workflow.

### Step 2: Initialize Connections
```
📋 Initializing database connections...
```
Establishes connections to both DynamoDB and PostgreSQL.

### Step 3: Setup PostgreSQL
```
📋 Setting up PostgreSQL...
✅ PostgreSQL ready - Table: powerbi_tractask
```
Creates or verifies the target table structure.

### Step 4: Extract from DynamoDB
```
📋 Fetching records from DynamoDB...
   Client ID: WAX671O
✅ Fetched 2440 total records from DynamoDB
```
Uses Query operation with pagination to fetch all records for the client.

### Step 5: Transform Data
```
📋 Transforming 2440 records...
✅ Transformation complete:
   - Valid records: 2440
   - Skipped records: 0
```
Applies transformations:
- Maps workflow-specific statuses
- Standardizes project names
- Extracts employee names
- Parses dates and times
- Calculates date dimensions (year, month, quarter)
- Auto-populates closed dates for completed tasks
- Handles NULL values

### Step 6: Check Duplicates
```
📋 Checking for duplicates...
✅ No duplicates found
```
Identifies and removes duplicate TrackingIDs, keeping the first occurrence.

### Step 7: Sync to PostgreSQL
```
📋 Syncing data to PostgreSQL...
✅ Sync complete - New: 50, Updated: 2390
```
Performs intelligent upsert operation:
- **INSERT**: New tracking IDs
- **UPDATE**: Existing records with changes

## Update Detection Logic

The ETL compares these fields to detect changes:
1. `status_id` - Status identifier
2. `status` - Status name (workflow-specific)
3. `employee` - Assigned employee
4. `workflowid` - Workflow identifier
5. `project_name` - Project categorization
6. `group_name` - Task type group
7. `priority` - Priority level
8. `deadline` / `deadtime` - Deadline date/time
9. `closeddate` / `closedtime` - Closed date/time

If ANY field changes, the record is updated with ALL current values.

## Database Schema

### Target Table: `powerbi_tractask`

| Column | Type | Description |
|--------|------|-------------|
| id | SERIAL | Primary key |
| tracking_id | VARCHAR(50) | Unique tracking identifier |
| workflowid | VARCHAR(50) | Workflow identifier |
| project_name | VARCHAR(200) | Standardized project name |
| group_name | VARCHAR(100) | Task type group |
| priority | VARCHAR(50) | High/Medium/Low |
| status | VARCHAR(100) | Workflow-specific status name |
| status_id | VARCHAR(10) | Status identifier |
| employee | VARCHAR(200) | Assigned employee name |
| reported_date | DATE | Task reported date |
| year | INTEGER | Year dimension |
| month | INTEGER | Month dimension (1-12) |
| month_name | VARCHAR(20) | Month name |
| year_month | VARCHAR(20) | YYYY-MM format |
| quarter | VARCHAR(10) | Q1/Q2/Q3/Q4 |
| year_quarter | VARCHAR(20) | YYYY-QX format |
| deadline | DATE | Deadline date (NULL if empty) |
| deadtime | TIME | Deadline time (NULL if empty) |
| closeddate | DATE | Closed date (NULL if empty) |
| closedtime | TIME | Closed time (NULL if empty) |
| created_at | TIMESTAMP | Record creation timestamp |
| updated_at | TIMESTAMP | Last update timestamp |

## Usage

### Run the ETL
```bash
python tractask_updated.py
```

### Diagnose Issues
```bash
python diagnose_types.py
```

### Expected Output
```
======================================================================
🚀 STARTING ETL PROCESS
======================================================================

📋 Step 1: Loading workflow configurations...
✅ Loaded 92 workflows

📋 Step 2: Initializing database connections...

📋 Step 3: Setting up PostgreSQL...
✅ PostgreSQL ready - Table: powerbi_tractask

📋 Step 4: Fetching records from DynamoDB...
✅ Fetched 2440 total records from DynamoDB

📋 Step 5: Transforming 2440 records...
✅ Transformation complete: 2440 valid records

📋 Step 6: Checking for duplicates...
✅ No duplicates found

📋 Step 7: Syncing data to PostgreSQL...
✅ Sync complete - New: 50, Updated: 2390

----------------------------------------------------------------------
RECORDS BY PROJECT:
----------------------------------------------------------------------
  ForesytsHr           :  1200 records
  TrackTask            :   450 records
  Insyts-BDO           :   350 records
  ForesytsPay          :   200 records
  DevOps               :   150 records
  Nexus                :    90 records
  ...

======================================================================
✅ SUCCESS!
   Database: Tractask
   Table: powerbi_tractask
   New Records Added: 50
   Records Updated: 2390
   Total Records: 2440
======================================================================
```

## Status Mapping Examples

### Workflow 19151 (Default Workflow)
```json
{
  "0": "Open",
  "1": "In Progress",
  "2": "Code Review",
  "3": "Ready for Testing",
  "4": "Re Open",
  "5": "Verified",
  "6": "Deployed",
  "7": "On hold",
  "8": "Cancelled",
  "9": " Closed",
  "10": "In Clarification",
  "11": "Ready for Deployment"
}
```

### Workflow 18477 (WailaSoftHR)
```json
{
  "0": "Open ",
  "1": "In Progress ",
  "2": "On Hold",
  "3": "Pending Review",
  "4": "Blocked",
  "5": "Rework ",
  "6": "Completed ",
  "7": "Closed "
}
```

Note: Status "2" means "Code Review" in workflow 19151 but "On Hold" in workflow 18477!

## Project Name Standardization

### Input → Output Mapping

#### Insyts Projects
```
"insyts"                    → Insyts-BDO
"insyts - bdo"              → Insyts-BDO
"insyts -bdo -development"  → Insyts-BDO
"insyts -bdo -global"       → Insyts-Global
"insyts amadi"              → Insyts-Amadi
```

#### ForesytsHr (formerly TymeplusHr)
```
"tymeplushr"                      → ForesytsHr
"tymeplushr - demolive"           → ForesytsHr
"tymeplushr - phoenix"            → ForesytsHr
"foresyts"                        → ForesytsHr
"foresytshr"                      → ForesytsHr
```

#### TrackTask
```
"tractask"                  → TrackTask
"tracktask"                 → TrackTask
"tractask - development"    → TrackTask
```

#### Nexus (New)
```
"nexus"                     → Nexus
```

#### Others
```
"lms"                       → Others
"kenya team"                → Others
```

## Type to Group Mapping

| Type | Group Name |
|------|------------|
| 1 | Bug |
| 2 | Issue |
| 3 | New Task |
| 4 | Request for Change |
| Other | Others |

## Troubleshooting

### Issue: "Fetched 0 records"
**Solution:** Run `diagnose_types.py` to check:
- Table structure
- Key attribute names
- Sample data format

### Issue: "All records filtered out"
**Solution:** This was fixed by removing the Type filter. All records are now processed.

### Issue: "Status shows as 'Unknown Status (X)'"
**Solution:** The workflow ID or status ID is not in workflow.json. Either:
- Update workflow.json with the missing workflow
- Check if the workflow ID is correct in DynamoDB

### Issue: "Project name showing as 'Others'"
**Solution:** Add the project name mapping to PROJECT_NAME_MAP in the script.

## Logging

The script provides detailed logging at each step:
- INFO: Normal operations and progress
- WARNING: Skipped records, missing data
- ERROR: Connection issues, critical failures

Logs include:
- Record counts at each stage
- Transformation progress (every 100 records)
- Update details (what changed)
- Final statistics summary

## Performance

- **Extraction**: ~2-3 seconds per 500 records
- **Transformation**: ~1 second per 1000 records
- **Loading**: ~5-10 seconds per 1000 updates

**Typical Run Time**: 30-60 seconds for 2500 records

## Data Quality Checks

1. ✅ Duplicate tracking ID detection
2. ✅ Date format validation
3. ✅ Required field validation (reported date)
4. ✅ NULL handling for optional fields
5. ✅ Employee name cleaning (removes titles, IDs)
6. ✅ Project name standardization

## Maintenance

### Adding New Workflows
1. Update `workflow.json` with new workflow definition
2. Run the ETL - it will automatically use new mappings

### Adding New Projects
1. Add mapping to `PROJECT_NAME_MAP` in the script
2. Run the ETL - existing records will be updated

### Updating Status Definitions
1. Modify workflow definitions in `workflow.json`
2. Run the ETL - records will be updated with new status names

## Dependencies

```
boto3>=1.26.0
pandas>=1.5.0
psycopg2-binary>=2.9.0
```

Install with:
```bash
pip install boto3 pandas psycopg2-binary
```

## Support

For issues or questions:
1. Check logs for error details
2. Run diagnostic script
3. Verify workflow.json is valid JSON
4. Check database connectivity

## Change Log

### Version 2.0 (Current)
- ✅ Removed Type filter (all records processed)
- ✅ Added workflow-based status mapping
- ✅ Enhanced update detection (project, workflow, all fields)
- ✅ Auto-populate closed dates for completed tasks
- ✅ Added Nexus, LMS, UIUX projects
- ✅ Split Insyts into BDO/Global/Amadi
- ✅ Improved NULL handling
- ✅ Enhanced logging and progress tracking

### Version 1.0
- Basic ETL with hardcoded status map
- Type filter (1,2,3,4 only)
- Limited update detection

### Notes & Best Practices

Recommended to run during low traffic hours

Ensure DynamoDB scan limits are acceptable for large datasets

Consider adding CloudWatch logging for production usage

### Author
Leo
Backend / Data Engineer
ETL • AWS • PostgreSQL • Python