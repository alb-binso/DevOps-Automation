"""
ETL for breaks:
- Type 1: derive from attendance logs breakin/breakout (device_id filter, current date, migration_status='No')
  - If both breakin and breakout exist: status = 'manual'
  - If only breakin exists (no breakout by 8 PM): status = 'auto', auto-add +30min as breakout
- Type 2: auto-insert for checked-in users without ANY break at 8 PM Kenya time
  - Status = 'auto', break time = 13:00-13:30
- Prevent duplicate break per user/day
- Update migration_status to 'Yes' after insert
"""

import datetime as dt
import uuid
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
import pytz

# --- CONFIG ---
KENYA_TZ = pytz.timezone('Africa/Nairobi')  # Kenya timezone (EAT - UTC+3)
TODAY = dt.date.today()  # adjust/testing override if needed
DEVICE_ID = "GED7243200199"
CLIENT_ID = "WEKOQEJ"
BREAK_ID_DEFAULT = 1
PUNCHIN_ID_DEFAULT = 3
PUNCHOUT_ID_DEFAULT = 4
AUTO_BREAK_START = dt.time(13, 0)
AUTO_BREAK_DURATION_MIN = 30
END_OF_DAY_TIME = dt.time(20, 0)  # 8 PM Kenya time - when to finalize breaks

# Database Configurations
SRC_DB = {

}

TGT_DB = {

}

# Derived connection strings
PG_URL = (
    f"postgresql+psycopg2://{SRC_DB['user']}:{SRC_DB['password']}"
    f"@{SRC_DB['host']}:{SRC_DB['port']}/{SRC_DB['dbname']}"
)  # tymeplushr_attendance_logs source

HR_URL = (
    f"postgresql+psycopg2://{TGT_DB['user']}:{TGT_DB['password']}"
    f"@{TGT_DB['host']}:{TGT_DB['port']}/{TGT_DB['dbname']}"
)  # biometrics_master, tymeplususerpunchactions, tymeplususerbreak

pg_engine: Engine = create_engine(PG_URL, future=True)
hr_engine: Engine = create_engine(HR_URL, future=True)

def next_breakpunchactionid(conn_hr):
    # Use max(existing)+1 to fit integer PK; replace with DB sequence if available.
    res = conn_hr.execute(
        text("SELECT COALESCE(MAX(breakpunchactionid), 0) + 1 FROM tymeplususerbreak")
    ).scalar_one()
    return int(res)

def fetch_attendance_break_logs(conn_pg):
    sql = """
    SELECT user_id, punch_type, punch_date, punch_time, migration_status
    FROM public.tymeplushr_attendance_logs
    WHERE device_id = :device_id
      AND punch_date = :punch_date
      AND migration_status = 'No'
      AND punch_type IN ('breakin', 'breakout')
    ORDER BY user_id, punch_time
    """
    rows = conn_pg.execute(
        text(sql), {"device_id": DEVICE_ID, "punch_date": TODAY}
    ).mappings().all()
    print(f"[TYPE1] Fetched {len(rows)} attendance break logs for {TODAY} and device {DEVICE_ID}")
    return rows

def fetch_user_punch_actions(conn_hr, user_ids):
    if not user_ids:
        return {}
    sql = """
    SELECT userid, employeeid, fullname, departmentid, departmentname,
           reportingmanager, checkindate, checkoutdate, checkouttime
    FROM tymeplususerpunchactions
    WHERE employeeid = ANY(:uids)
      AND checkindate = :punch_date
    """
    rows = conn_hr.execute(
        text(sql), {"uids": list(user_ids), "punch_date": TODAY}
    ).mappings().all()
    print(f"[TYPE1] Loaded punch actions for {len(rows)} employees: {list(user_ids)}")
    return {r["employeeid"]: r for r in rows}

def existing_break_for_day(conn_hr, employeeid):
    sql = """
    SELECT 1 FROM tymeplususerbreak
    WHERE employeeid = :emp AND breakstartdate = :pdate
    LIMIT 1
    """
    return conn_hr.execute(text(sql), {"emp": employeeid, "pdate": TODAY}).first() is not None


def _to_date(val):
    if isinstance(val, dt.date):
        return val
    if isinstance(val, str):
        try:
            return dt.date.fromisoformat(val)
        except ValueError:
            pass
    raise TypeError(f"Unsupported date value: {val!r}")


def _to_time(val):
    if isinstance(val, dt.time):
        return val
    if isinstance(val, str):
        try:
            return dt.time.fromisoformat(val)
        except ValueError:
            # Fallback for strings with timezone/space
            try:
                return dt.datetime.fromisoformat(val).time()
            except ValueError:
                pass
    raise TypeError(f"Unsupported time value: {val!r}")


def _to_datetime(date_val, time_val):
    return dt.datetime.combine(_to_date(date_val), _to_time(time_val))

def insert_break(conn_hr, meta, start_dt, end_dt, break_status):
    breakpunchactionid = next_breakpunchactionid(conn_hr)
    print(
        f"[INSERT] employeeid={meta['employeeid']} userid={meta['userid']} "
        f"breakpunchactionid={breakpunchactionid} "
        f"start={start_dt} end={end_dt} status={break_status}"
    )
    conn_hr.execute(
        text("""
        INSERT INTO tymeplususerbreak (
            userid, breakpunchactionid, employeeid, clientid, fullname,
            departmentid, departmentname, breakid, breakpunchinid, breakstartdatetime,
            breakstartdate, breakstarttime, reportingmanager,
            breakpunchoutid, breakenddatetime, breakenddate, breakendtime, break_status
        ) VALUES (
            :userid, :breakpunchactionid, :employeeid, :clientid, :fullname,
            :departmentid, :departmentname, :breakid, :breakpunchinid, :breakstartdatetime,
            :breakstartdate, :breakstarttime, :reportingmanager,
            :breakpunchoutid, :breakenddatetime, :breakenddate, :breakendtime, :break_status
        )
        """),
        {
            "userid": meta["userid"],
            "breakpunchactionid": breakpunchactionid,
            "employeeid": meta["employeeid"],
            "clientid": CLIENT_ID,
            "fullname": meta["fullname"],
            "departmentid": meta["departmentid"],
            "departmentname": meta["departmentname"],
            "breakid": BREAK_ID_DEFAULT,
            "breakpunchinid": PUNCHIN_ID_DEFAULT,
            "breakstartdatetime": start_dt,
            "breakstartdate": start_dt.date(),
            "breakstarttime": start_dt.time(),
            "reportingmanager": meta["reportingmanager"],
            "breakpunchoutid": PUNCHOUT_ID_DEFAULT,
            "breakenddatetime": end_dt,
            "breakenddate": end_dt.date(),
            "breakendtime": end_dt.time(),
            "break_status": break_status,
        }
    )

def mark_attendance_migrated(conn_pg, user_id, punch_time):
    conn_pg.execute(
        text("""
        UPDATE public.tymeplushr_attendance_logs
        SET migration_status = 'Yes'
        WHERE user_id = :uid AND punch_date = :pdate AND punch_time = :ptime
          AND device_id = :device_id
        """),
        {"uid": user_id, "pdate": TODAY, "ptime": punch_time, "device_id": DEVICE_ID},
    )

def is_end_of_day():
    """Check if current Kenya time is at or past 8 PM"""
    kenya_now = dt.datetime.now(KENYA_TZ)
    return kenya_now.time() >= END_OF_DAY_TIME

def process_type1(conn_pg, conn_hr):
    """
    Process break punches from attendance logs:
    - Accept any combination: breakin/breakin, breakout/breakout, breakin/breakout, breakout/breakin
    - If 2+ punches exist: use first and second as start/end, status='manual'
    - If only 1 punch exists and it's 8 PM: use it as start, auto-add +30min as end, status='auto'
    - If only 1 punch exists and it's before 8 PM: skip (wait until 8 PM)
    """
    logs = fetch_attendance_break_logs(conn_pg)
    # group by user
    by_user = {}
    for row in logs:
        by_user.setdefault(row["user_id"], []).append(row)
    user_meta = fetch_user_punch_actions(conn_hr, by_user.keys())

    print(f"[TYPE1] Processing {len(by_user)} users with break punches")
    end_of_day = is_end_of_day()
    print(f"[TYPE1] End of day (8 PM) reached: {end_of_day}")

    for uid, punches in by_user.items():
        meta = user_meta.get(uid)
        if not meta or not meta["checkindate"]:
            print(f"[TYPE1] Skipping user {uid}: no punch action/checkin for {TODAY}")
            continue  # skip if no checkin that day
        
        # Prevent duplicate break per day
        if existing_break_for_day(conn_hr, uid):
            print(f"[TYPE1] User {uid} already has a break for {TODAY}, skipping")
            continue
        
        # sort punches by time
        punches.sort(key=lambda r: r["punch_time"])
        
        if len(punches) >= 2:
            # User has 2 or more punches - use first two regardless of type
            first_punch = punches[0]
            second_punch = punches[1]
            
            start_date = _to_date(first_punch["punch_date"])
            start_time = _to_time(first_punch["punch_time"])
            start_dt = dt.datetime.combine(start_date, start_time)
            
            end_date = _to_date(second_punch["punch_date"])
            end_time = _to_time(second_punch["punch_time"])
            end_dt = dt.datetime.combine(end_date, end_time)
            
            # Ensure end is after start and within reasonable time (2 hours)
            if end_dt > start_dt and end_dt <= start_dt + dt.timedelta(hours=2):
                insert_break(conn_hr, meta, start_dt, end_dt, "manual")
                mark_attendance_migrated(conn_pg, uid, first_punch["punch_time"])
                mark_attendance_migrated(conn_pg, uid, second_punch["punch_time"])
                print(f"[TYPE1] User {uid}: Two punches found ({first_punch['punch_type']}/{second_punch['punch_type']}), status='manual'")
            elif end_dt <= start_dt:
                print(f"[TYPE1] User {uid}: Second punch time is not after first punch, skipping")
            else:
                print(f"[TYPE1] User {uid}: Punch times too far apart (>2 hours), skipping")
                
        elif len(punches) == 1:
            # User has only 1 punch (breakin or breakout)
            single_punch = punches[0]
            start_date = _to_date(single_punch["punch_date"])
            start_time = _to_time(single_punch["punch_time"])
            start_dt = dt.datetime.combine(start_date, start_time)
            
            if end_of_day:
                # It's 8 PM - auto-calculate end as +30 minutes, status = 'auto'
                end_dt = start_dt + dt.timedelta(minutes=AUTO_BREAK_DURATION_MIN)
                insert_break(conn_hr, meta, start_dt, end_dt, "auto")
                mark_attendance_migrated(conn_pg, uid, single_punch["punch_time"])
                print(f"[TYPE1] User {uid}: Only one punch ({single_punch['punch_type']}) found, auto-added +30min end, status='auto'")
            else:
                # Before 8 PM - wait for potential second punch
                print(f"[TYPE1] User {uid}: Only one punch ({single_punch['punch_type']}) found, waiting until 8 PM for potential second punch")

def process_type2(conn_pg, conn_hr):
    """
    Auto-insert breaks for users who haven't taken ANY break:
    - Only runs at 8 PM Kenya time
    - Status = 'auto', break time = 13:00-13:30
    """
    # Only run at end of day (8 PM Kenya time)
    if not is_end_of_day():
        kenya_now = dt.datetime.now(KENYA_TZ)
        print(f"[TYPE2] Current Kenya time: {kenya_now.strftime('%H:%M:%S')} - waiting until {END_OF_DAY_TIME} to auto-insert breaks")
        return
    
    kenya_now = dt.datetime.now(KENYA_TZ)
    print(f"[TYPE2] End of day reached ({kenya_now.strftime('%Y-%m-%d %H:%M:%S %Z')}) - processing auto-breaks")
    
    # Get checked-in users for today
    checked_in = conn_hr.execute(
        text("""
        SELECT userid, employeeid, fullname, departmentid, departmentname,
               reportingmanager
        FROM tymeplususerpunchactions
        WHERE checkindate = :pdate
        """),
        {"pdate": TODAY},
    ).mappings().all()

    print(f"[TYPE2] Found {len(checked_in)} checked-in users for {TODAY}")

    for meta in checked_in:
        if existing_break_for_day(conn_hr, meta["employeeid"]):
            print(f"[TYPE2] User {meta['employeeid']} already has a break, skipping auto-insert")
            continue
        
        # User has no break at all - insert auto break at default time
        start_dt = dt.datetime.combine(TODAY, AUTO_BREAK_START)
        end_dt = start_dt + dt.timedelta(minutes=AUTO_BREAK_DURATION_MIN)
        insert_break(conn_hr, meta, start_dt, end_dt, "auto")
        print(f"[TYPE2] User {meta['employeeid']}: No break found, auto-inserted break at {AUTO_BREAK_START}, status='auto'")

def main():
    print(f"[MAIN] Starting break ETL for date {TODAY}")
    kenya_now = dt.datetime.now(KENYA_TZ)
    print(f"[MAIN] Current Kenya time: {kenya_now.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    
    with pg_engine.begin() as conn_pg, hr_engine.begin() as conn_hr:
        process_type1(conn_pg, conn_hr)
        process_type2(conn_pg, conn_hr)
    print("[MAIN] Break ETL completed")

if __name__ == "__main__":
    main()