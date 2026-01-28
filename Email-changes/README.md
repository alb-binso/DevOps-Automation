📊 Architecture & Flow Diagram
1️⃣ High-Level Overview

This script synchronizes email / user identity changes across:

Multiple PostgreSQL databases

Multiple AWS DynamoDB tables

A LIVE DynamoDB environment (userAuthentication)

All updates are driven by email_mappings.json.

2️⃣ High-Level Data Flow (ASCII Diagram)
                   ┌───────────────────────────┐
                   │   email_mappings.json     │
                   │  (old → new emails)       │
                   └─────────────┬─────────────┘
                                 │
                                 ▼
        ┌──────────────────────────────────────────┐
        │        Email Migration Script              │
        │                                          │
        │  - PostgreSQL updates                     │
        │  - DynamoDB updates (non-live)            │
        │  - Live DynamoDB updates (confirmed)      │
        │                                          │
        │  Uses:                                   │
        │  - psycopg2                               │
        │  - boto3                                  │
        │  - dotenv                                 │
        └─────────────┬─────────────┬─────────────┘
                      │             │
                      ▼             ▼
     ┌────────────────────────┐   ┌────────────────────────┐
     │     PostgreSQL DBs      │   │     DynamoDB (Non-Live)│
     │                        │   │                        │
     │ tymeplushr-* databases │   │ Multiple HR tables     │
     │                        │   │ Email / UserId fields  │
     └────────────────────────┘   └─────────────┬──────────┘
                                                  │
                                                  ▼
                                   ┌────────────────────────┐
                                   │  LIVE DynamoDB          │
                                   │  userAuthentication     │
                                   │  (High-risk operation) │
                                   └────────────────────────┘

3️⃣ PostgreSQL Update Flow
For each PostgreSQL database
        │
        ▼
Ask user confirmation
        │
        ▼
For each table
        │
        ▼
For each column
        │
        ▼
For each email mapping
        │
        ├─► UPDATE old_email → new_email
        │
        ├─► If UPDATE fails but old exists
        │        └─► DELETE row
        │
        └─► If old not found → SKIP


✔ Handles multiple DBs
✔ Per-column tracking
✔ Safe commits per database

4️⃣ DynamoDB (Non-Live) Update Logic
Scan DynamoDB Table
        │
        ▼
For each item
        │
        ▼
For each email field
        │
        ├─► Single-value field
        │        ├─ Create new item
        │        ├─ Merge missing fields
        │        └─ Delete old item
        │
        └─► Array/List field
                 └─ Replace email inside array


✔ Handles:

Single email attributes

Array / list email attributes
✔ Preserves non-null fields
✔ Safe merge + delete pattern

5️⃣ LIVE DynamoDB (userAuthentication) Flow ⚠️
User Confirmation Required
        │
        ▼
Scan LIVE userAuthentication
        │
        ▼
Match old email (+ optional clientId)
        │
        ▼
Check if new email already exists
        │
        ├─► EXISTS
        │      ├─ Merge missing fields
        │      └─ Update existing item
        │
        └─► NOT EXISTS
               └─ Create new item
        │
        ▼
Verify data integrity
        │
        ├─► Match → Delete old item
        └─► Mismatch → Keep both (manual review)


🔒 Safety Features:

Explicit YES confirmation

Field-by-field comparison

Deletes only after verification

Client-scoped updates supported

6️⃣ GitHub-Rendered Mermaid Diagram

✅ Best for GitHub / GitLab READMEs

flowchart TD
    MAP[email_mappings.json]

    MAP --> SCRIPT[Migration Script]

    SCRIPT --> PG[PostgreSQL Databases]
    SCRIPT --> DDB[DynamoDB Non-Live]
    SCRIPT --> LIVE[LIVE DynamoDB userAuthentication]

    PG --> PG1[UPDATE email]
    PG --> PG2[DELETE old rows]

    DDB --> D1[Merge Items]
    DDB --> D2[Array Email Update]

    LIVE --> L1[Create or Merge]
    LIVE --> L2[Verify]
    LIVE --> L3[Delete Old Item]

7️⃣ Execution Order
1. Load environment variables
2. Load email mappings
3. Update PostgreSQL databases
4. Update DynamoDB (non-live)
5. Confirm & update LIVE DynamoDB
6. Print final migration summary