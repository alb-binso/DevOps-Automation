### DynamoDB to PostgreSQL ETL
### Overview

This project is a Python-based ETL (Extract, Transform, Load) script that migrates data from AWS DynamoDB tables into a PostgreSQL database.

It is designed for the Tymeplus / Foresyts HR domain, handling multiple master and transactional tables while preserving referential integrity, handling pagination, and performing upserts (insert or update on conflict).

### Features

✅ Secure configuration using .env

✅ Automatic PostgreSQL table creation

✅ DynamoDB pagination handling

✅ Decimal to native type conversion

✅ Foreign key dependency handling

✅ Idempotent migrations using ON CONFLICT

✅ Skips invalid or inactive employee records safely

✅ Detailed console logs for tracking progress

Data Flow (High Level)
DynamoDB Tables
   ↓
Data Cleaning & Type Conversion
   ↓
PostgreSQL Tables (Upsert)

DynamoDB Tables Used
DynamoDB Table	Purpose
tymeplusDepartmentMaster	Department master data
tymeplusLeaveCategories	Leave type master
tymeplusHolidayMaster	Holiday calendar
tymeplusUserAuth	Employee master
tymeplusUserLeaves	Leave transactions
tymeplusUserAbsentList	Absent records
PostgreSQL Tables Created
PostgreSQL Table	Description
foresytsdepartment	Department master
foresytleavetype	Leave category master
foresytsholidays	Holidays
foresytsemployee_master	Employee master
foresytsleavelist	Leave records
foresytsabsentlist	Absent records

Tables are created automatically if they do not exist.

Prerequisites

Python 3.8+

AWS credentials with DynamoDB read access

PostgreSQL 12+

Network access to both DynamoDB and PostgreSQL

Python Dependencies

Install required packages:

pip install boto3 psycopg2-binary python-dotenv

Environment Configuration

Create a file named foremain.env in the project root.

# AWS Configuration
AWS_REGION=ap-south-1
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key

# PostgreSQL Configuration
PG_HOST=localhost
PG_PORT=5432
PG_DATABASE=your_db
PG_USER=your_user
PG_PASSWORD=your_password

# Client Configuration
CLIENT_ID=WASJKSP
📊 ETL Architecture Diagram
1️⃣ High-Level Data Flow (ASCII)
                ┌──────────────────────┐
                │   foremain.env        │
                │ (AWS + PG configs)   │
                └──────────┬───────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────┐
│        DynamoDBToPostgresETL (Python)             │
│                                                  │
│  - boto3 (DynamoDB)                               │
│  - psycopg2 (PostgreSQL)                          │
│  - dotenv                                         │
│                                                  │
│  - Decimal → native type conversion               │
│  - Date & timestamp parsing                       │
│  - Pagination handling                            │
│  - Upserts with ON CONFLICT                       │
└───────────────┬──────────────────────────────────┘
                │
                ▼
┌──────────────────────────────────────────────────┐
│                AWS DynamoDB                      │
│                                                  │
│  tymeplusDepartmentMaster                        │
│  tymeplusLeaveCategories                         │
│  tymeplusHolidayMaster                           │
│  tymeplusUserAuth                                │
│  tymeplusUserLeaves                              │
│  tymeplusUserAbsentList                          │
└───────────────┬──────────────────────────────────┘
                │
                ▼
┌──────────────────────────────────────────────────┐
│                PostgreSQL                         │
│                                                  │
│  foresytsdepartment                               │
│  foresytleavetype                                 │
│  foresytsholidays                                 │
│  foresytsemployee_master                          │
│  foresytsleavelist                                │
│  foresytsabsentlist                               │
└──────────────────────────────────────────────────┘

2️⃣ Migration Dependency Flow

This shows why the migration order matters:

Departments
     │
     ▼
Employees ───────────────┐
     │                   │
     ▼                   ▼
Leave Types           Holidays
     │                   │
     └─────────┬─────────┘
               ▼
            Leaves
               │
               ▼
            Absents


✔ Foreign keys are respected
✔ Invalid employees are skipped safely

3️⃣ GitHub-Rendered Diagram (Mermaid)

✅ GitHub automatically renders this
❌ Do NOT use on platforms without Mermaid support

flowchart TD
    ENV[foremain.env] --> ETL[DynamoDBToPostgresETL]

    ETL --> D1[tymeplusDepartmentMaster]
    ETL --> D2[tymeplusLeaveCategories]
    ETL --> D3[tymeplusHolidayMaster]
    ETL --> D4[tymeplusUserAuth]
    ETL --> D5[tymeplusUserLeaves]
    ETL --> D6[tymeplusUserAbsentList]

    D1 --> P1[foresytsdepartment]
    D2 --> P2[foresytleavetype]
    D3 --> P3[foresytsholidays]
    D4 --> P4[foresytsemployee_master]
    D5 --> P5[foresytsleavelist]
    D6 --> P6[foresytsabsentlist]

4️⃣ ETL Execution Sequence Diagram
Start
  │
  ▼
Load Environment Variables
  │
  ▼
Connect to PostgreSQL
  │
  ▼
Create Tables (if not exists)
  │
  ▼
Migrate Departments
  │
  ▼
Migrate Leave Types
  │
  ▼
Migrate Holidays
  │
  ▼
Migrate Employees
  │
  ▼
Migrate Leaves (validate employee)
  │
  ▼
Migrate Absents (validate employee)
  │
  ▼
Close Connections
  │
  ▼
End

How to Run
python etl.py


Replace etl.py with the actual filename if different.

Execution Order (Important)

The script migrates data in dependency-safe order:

Departments

Leave Types

Holidays

Employees

Leaves

Absents

This prevents foreign key violations.

Key Behaviors
Decimal Handling

DynamoDB Decimal values are automatically converted to int or float.

Date & Timestamp Parsing

Supports:

ISO timestamps (2024-01-01T10:00:00Z)

Date-only strings (YYYY-MM-DD)

Invalid or empty values are safely converted to NULL.

Employee Validation

Leave and absent records are skipped if the employee does not exist or is inactive.

Skipped records are logged clearly.

Error Handling

Any failure:

Rolls back PostgreSQL transactions

Prints a clear error message

Database connections are always closed safely.

Sample Console Output
============================================================
### DynamoDB to PostgreSQL ETL Process
============================================================
✓ Connected to PostgreSQL
✓ Tables created/verified

→ Migrating departments...
✓ Migrated 12 departments

→ Migrating employees...
✓ Migrated 245 employees

→ Migrating leaves...
⚠ Skipping leave record for non-existent/inactive employee: U123
✓ Migrated 1,420 leave records

### ✓ ETL Process Completed Successfully!
============================================================
✓ Database connections closed

Customization

You can easily:

Change CLIENT_ID to migrate a different tenant

Disable specific migrations by commenting methods in run()

Schedule execution via cron / Airflow / Jenkins

### Notes & Best Practices

Recommended to run during low traffic hours

Ensure DynamoDB scan limits are acceptable for large datasets

Consider adding CloudWatch logging for production usage

### Author
Leo
Backend / Data Engineer
ETL • AWS • PostgreSQL • Python