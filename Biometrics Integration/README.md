📊 Attendance Processing System – Diagrams
1️⃣ High-Level Architecture Diagram
┌─────────────────────────────┐
│   Source PostgreSQL DB      │
│                             │
│ tymeplushr_attendance_logs  │
│ (raw checkin / checkout)    │
└──────────────┬──────────────┘
               │
               ▼
┌─────────────────────────────────────────────┐
│        AttendanceProcessor (Python)          │
│                                             │
│ • Incremental sync                           │
│ • Validation & cleanup                       │
│ • Cross-day handling                         │
│ • Auto checkout logic                        │
│ • Deduplication                              │
│                                             │
│ Thread-safe | Transaction controlled         │
└──────────────┬──────────────┬───────────────┘
               │              │
               │              ▼
               │   ┌─────────────────────────┐
               │   │ Target PostgreSQL DB     │
               │   │                          │
               │   │ biometrics_master        │
               │   │ test_tymeplususerpunch… │
               │   └─────────────────────────┘
               │
               ▼
┌─────────────────────────────┐
│  Migration Status Update   │
│  (migration_status = Yes)  │
└─────────────────────────────┘

2️⃣ Incremental Sync Flow (Main Logic)
sync_incremental()
        │
        ▼
ensure_connection()
        │
        ▼
find_and_fix_mismatched_data()
        │
        ▼
validate_and_fix_time_anomalies()
        │
        ▼
get_date_range_to_process()
        │
        ▼
handle_cross_day_scenarios()
        │
        ▼
FOR each date
    └─► process_attendance_by_date()
        │
        ▼
process_auto_checkouts_for_incomplete_records()
        │
        ▼
validate_and_cleanup_duplicates()
        │
        ▼
Update last_sync_time

3️⃣ Check-In Processing Flow
process_checkin()
        │
        ▼
Already processed?
        ├─► YES → SKIP
        └─► NO
              │
              ▼
        Fetch biometrics
              │
              ├─► Not found → SKIP
              │
              ▼
        Check existing checkin
              │
              ├─► Exists → SKIP
              │
              ▼
        Calculate punch status
        (ontime / late)
              │
              ▼
        INSERT checkin record
              │
              ▼
        Update migration_status = 'Yes'
              │
              ▼
        Look for checkout
              │
              ├─► Found → process_checkout()
              │
              └─► Not found
                    │
                    ├─► Past date → Auto-checkout @ 05:00
                    └─► Today → Leave incomplete

4️⃣ Checkout Processing Flow
process_checkout()
        │
        ▼
Already processed?
        ├─► YES → SKIP
        └─► NO
              │
              ▼
        Fetch biometrics
              │
              ▼
        Find matching checkin
              │
              ├─► Not found → SKIP
              │
              ▼
        Validate time logic
        (checkout > checkin)
              │
              ├─► Invalid → SKIP
              │
              ▼
        Determine status
        ├─ Early → earlycheckout
        ├─ Shift end passed → manual / overtime
        └─ Forced → auto
              │
              ▼
        UPDATE checkout fields
              │
              ▼
        COMMIT

5️⃣ Cross-Day (Night Shift) Scenario
Day 1
┌──────────────┐
│ Checkin 22:30│
└──────┬───────┘
       │
       ▼
Day 2
┌──────────────┐
│ Checkout 03:45│
└──────────────┘
       │
       ▼
Detected as cross-day
       │
       ▼
Validate chronological order
       │
       ▼
Attach checkout to previous day

6️⃣ Auto-Checkout Logic (Critical)
Incomplete checkin found
        │
        ▼
Next day 05:00 AM passed?
        ├─► NO → Wait
        └─► YES
              │
              ▼
        Actual checkout exists?
              ├─► YES → process_checkout()
              └─► NO
                    │
                    ▼
              Auto checkout @ 05:00
              (force_auto = True)

7️⃣ Data Cleanup & Validation Flow
Pre-processing cleanup
        │
        ├─ Orphan checkouts → DELETE
        ├─ Checkout < Checkin → NULLIFY checkout
        ├─ Early morning mismatch → FIX
        └─ Duplicate checkins → KEEP oldest

Post-processing cleanup
        │
        ├─ Duplicate checkins → DELETE
        └─ Duplicate checkouts → DELETE

8️⃣ Mermaid Diagram (GitHub / Confluence Friendly)
flowchart TD
    SRC[Source DB<br/>attendance_logs]
    PROC[AttendanceProcessor]
    BIO[biometrics_master]
    TGT[target punch actions]

    SRC --> PROC
    PROC --> BIO
    PROC --> TGT

    PROC --> C1[Check-in Logic]
    PROC --> C2[Checkout Logic]
    PROC --> C3[Cross-day Handling]
    PROC --> C4[Auto Checkout @ 05:00]
    PROC --> C5[Cleanup & Dedup]

9️⃣ What This Diagram Communicates Clearly

✔ Incremental & safe processing
✔ Night shifts handled correctly
✔ No duplicate records
✔ Auto-recovery from missing punches
✔ Production-grade validation logic


### Author
Leo
Backend / Data Engineer
ETL • AWS • PostgreSQL • Python