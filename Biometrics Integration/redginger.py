# Database and data processing imports
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timedelta, time
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from typing import Dict, List, Optional, Tuple
from collections import defaultdict

# Source database configuration (where attendance logs are stored)
SRC_DB = {
    'dbname': '',
    'user': '',
    'password': '',
    'host': '',
    'port': 5432
}

# Target database configuration (where processed attendance data is stored)
TGT_DB = {
    'dbname': '',
    'user': '',
    'password': '',
    'host': '',
    'port': 5432
}

# Default values for attendance records (location, status, etc.)
DEFAULT_VALUES = {
    'loginlatitude': -1.2605476,      # Default latitude for check-in location
    'loginlocationid': 2,              # Default location ID for check-in
    'loginlocationname': 'Red Ginger Eatery Ltd',  # Default location name
    'loginlongitude': 36.816796,       # Default longitude for check-in location
    'logoutlatitude': -1.2605476,      # Default latitude for check-out location
    'logoutlocationid': 2,             # Default location ID for check-out
    'logoutlocationname': 'Red Ginger Eatery Ltd', # Default location name for check-out
    'logoutlongitude': 36.816796,      # Default longitude for check-out location
    'punchinid': 1,                    # Default punch-in ID
    'punchoutid': 2,                   # Default punch-out ID
    'skipdistancechecking': False,     # Whether to skip distance validation
    'statusid': 1                      # Default status ID (active)
}

# Mapping of department IDs to department names
DEPARTMENT_MAP = {
    '1': 'Service Bar',      # Department ID 1
    '2': 'Service Banda',    # Department ID 2
    '3': 'Kitchen',          # Department ID 3
    '4': 'Service Restaurant', # Department ID 4
    '5': 'Back Office',      # Department ID 5
    '6': 'Management'        # Department ID 6
}

class AttendanceProcessor:
    """
    Main class for processing attendance data from source database to target database.
    Handles check-in/check-out logic, time validation, and data synchronization.
    """
    def __init__(self):
        self.src_conn = None          # Source database connection
        self.tgt_conn = None          # Target database connection
        self.lock = threading.Lock()  # Thread lock for concurrent operations
        self.processed_records = set() # Track processed records to avoid duplicates
        self.last_sync_time = None    # Track last synchronization time
        self.debug_mode = True        # Enable debug prints for troubleshooting
    def resolve_overtime_flag(self, biometrics: Optional[Dict]) -> int:
        """
        Convert overtimestatus flag from biometrics data into an integer ID that
        downstream attendance records rely on (0 => no overtime, 1 => overtime).
        """
        status = (biometrics or {}).get('overtimestatus', 'no')
        return 1 if str(status).lower() == 'yes' else 0


    def log(self, message: str):
        """
        Print debug messages with timestamp if debug mode is enabled.
        Used for tracking the processing flow and troubleshooting.
        """
        if self.debug_mode:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"[{timestamp}] {message}")

    def connect_databases(self):
        """
        Establish connections to both source and target databases.
        Closes existing connections before creating new ones to ensure clean state.
        """
        self.log("Connecting to databases...")
        # Close existing connections if any
        if self.src_conn:
            self.src_conn.close()
        if self.tgt_conn:
            self.tgt_conn.close()
        # Create new connections
        self.src_conn = psycopg2.connect(**SRC_DB)
        self.tgt_conn = psycopg2.connect(**TGT_DB)
        # Disable autocommit for transaction control
        self.src_conn.autocommit = False
        self.tgt_conn.autocommit = False
        self.log("Database connections established successfully")

    def close_connections(self):
        """
        Safely close database connections to free up resources.
        Called when processing is complete or on error.
        """
        self.log("Closing database connections...")
        if self.src_conn:
            self.src_conn.close()
        if self.tgt_conn:
            self.tgt_conn.close()
        self.log("Database connections closed")

    def ensure_connection(self):
        """
        Verify database connections are still active and reconnect if needed.
        This prevents connection timeout errors during long-running operations.
        """
        try:
            # Test source database connection
            with self.src_conn.cursor() as cursor:
                cursor.execute("SELECT 1")
            # Test target database connection
            with self.tgt_conn.cursor() as cursor:
                cursor.execute("SELECT 1")
        except:
            self.log("Connection lost, reconnecting...")
            self.connect_databases()

    def get_biometrics_data(self, user_id: str) -> Optional[Dict]:
        """
        Retrieve employee biometrics data from target database.
        This includes shift times, employee details, and overtime status.
        Returns None if employee not found or inactive.
        """
        try:
            with self.tgt_conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT shiftstarttime, shiftendtime, clientid, employeeid, 
                           fullname, reportingmanager, userid, basedepartmentid,
                           COALESCE(overtimestatus, 'no') as overtimestatus
                    FROM biometrics_master 
                    WHERE employeeid = %s AND statusid = '1'
                    LIMIT 1
                """, (user_id,))
                result = cursor.fetchone()
                if result:
                    self.log(f"Found biometrics data for user {user_id}: {result['fullname']}")
                else:
                    self.log(f"No biometrics data found for user {user_id}")
                return result
        except Exception as e:
            self.log(f"Error getting biometrics data for user {user_id}: {e}")
            return None

    def parse_time_string(self, time_str: str) -> time:
        """
        Convert time string to time object for comparison operations.
        Handles various time formats and returns midnight (00:00:00) for invalid times.
        """
        try:
            if isinstance(time_str, str):
                if ':' in time_str:
                    parts = time_str.split(':')
                    hour = int(parts[0])
                    minute = int(parts[1])
                    second = int(parts[2]) if len(parts) > 2 else 0
                    return time(hour, minute, second)
            elif isinstance(time_str, time):
                return time_str
            return time(0, 0, 0)  # Return midnight for invalid times
        except:
            return time(0, 0, 0)  # Return midnight for any parsing errors

    def calculate_punch_status(self, punch_time: str, shift_time: str, is_checkin: bool) -> Tuple[str, str]:
        """
        Calculate attendance status based on punch time vs expected shift time.
        
        For check-ins:
        - 'ontime' if within 10 minutes of shift start time
        - 'late' if more than 10 minutes after shift start time
        
        For check-outs:
        - 'auto' if after or at shift end time
        - 'earlycheckout' if before shift end time
        """
        try:
            punch_t = self.parse_time_string(punch_time)
            shift_t = self.parse_time_string(shift_time)
            if is_checkin:
                # Add 10-minute grace period for check-ins
                grace_time = (datetime.combine(datetime.today(), shift_t) + timedelta(minutes=10)).time()
                if punch_t <= grace_time:
                    return 'ontime', ''
                else:
                    return 'late', 'late'
            else:
                # For check-outs, compare against shift end time
                if punch_t >= shift_t:
                    return 'auto', ''
                else:
                    return 'earlycheckout', 'earlycheckout'
        except:
            return 'auto', ''  # Default to auto status on error

    def create_record_key(self, user_id: str, punch_date, punch_time: str, punch_type: str) -> str:
        """
        Create a unique key for tracking processed records.
        Used to prevent duplicate processing of the same attendance record.
        """
        return f"{user_id}_{punch_date}_{punch_time}_{punch_type}"

    def get_attendance_logs_by_date_range(self, start_date, end_date) -> List[Dict]:
        """
        Retrieve attendance logs from source database for a specific date range.
        Only fetch records where migration_status = 'No'.
        Orders records chronologically with check-ins processed before check-outs.
        Enforces minimum date of August 1st, 2025 to avoid processing old data.
        """
        try:
            # Set minimum date to August 1st, 2025
            min_date = datetime(2025, 8, 1).date()
            if start_date < min_date:
                start_date = min_date
                self.log(f"Adjusted start date to minimum date: {start_date}")
            
            with self.src_conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT user_id, punch_date, punch_time, punch_type, device_id
                    FROM tymeplushr_attendance_logs
                    WHERE punch_date >= %s AND punch_date <= %s AND migration_status = 'No'
                    ORDER BY punch_date, punch_time, 
                             CASE WHEN punch_type = 'checkin' THEN 1 ELSE 2 END
                """, (start_date, end_date))
                records = cursor.fetchall()
                self.log(f"Retrieved {len(records)} attendance records from {start_date} to {end_date}")
                return records
        except Exception as e:
            self.log(f"Error getting attendance logs: {e}")
            return []

    def validate_and_fix_time_anomalies(self):
        """
        Detect and fix time anomalies in attendance records.
        Handles cases where checkout time is earlier than checkin time on the same day,
        which indicates the checkout belongs to a previous day's checkin.
        """
        try:
            self.log("Starting time anomaly validation and cleanup...")
            
            # Set minimum date to August 1st, 2025
            min_date = datetime(2025, 8, 1).date()
            
            with self.tgt_conn.cursor(cursor_factory=RealDictCursor) as cursor:
                # Find records where checkout time is earlier than checkin time on the same day
                cursor.execute("""
                    SELECT userpunchactionid, employeeid, checkindate, checkintime, checkoutdate, checkouttime
                    FROM test_tymeplususerpunchactions
                    WHERE checkintime IS NOT NULL 
                    AND checkouttime IS NOT NULL
                    AND checkindate = checkoutdate
                    AND checkouttime < checkintime
                    AND checkindate >= %s
                    ORDER BY checkindate DESC
                """, (min_date,))
                time_anomalies = cursor.fetchall()
                
                if time_anomalies:
                    self.log(f"Found {len(time_anomalies)} time anomalies (checkout time earlier than checkin time)")
                    
                    for anomaly in time_anomalies:
                        employee_id = anomaly['employeeid']
                        checkin_date = anomaly['checkindate']
                        checkin_time = anomaly['checkintime']
                        checkout_time = anomaly['checkouttime']
                        
                        self.log(f"Analyzing time anomaly: Employee {employee_id} on {checkin_date}")
                        self.log(f"  Checkin: {checkin_time}, Checkout: {checkout_time} (IMPOSSIBLE!)")
                        
                        # This checkout belongs to the previous day's checkin, not current day
                        # Remove the checkout data from current day and wait for proper checkout
                        self.log(f"  Checkout time is earlier than checkin - this checkout belongs to previous day")
                        self.log(f"  Removing checkout data from current day, waiting for proper checkout")
                        
                        cursor.execute("""
                            UPDATE test_tymeplususerpunchactions
                            SET checkoutdate = NULL, checkouttime = NULL, checkoutdatetime = NULL,
                                punchoutid = NULL, logoutlatitude = NULL, logoutlocationid = NULL,
                                logoutlocationname = NULL, logoutlongitude = NULL,
                                punchoutreason = NULL, punchoutstatus = NULL
                            WHERE userpunchactionid = %s
                        """, (anomaly['userpunchactionid'],))
                
                # Also find records where checkout time is very early (before 6 AM) on the same day as checkin
                cursor.execute("""
                    SELECT userpunchactionid, employeeid, checkindate, checkintime, checkoutdate, checkouttime
                    FROM test_tymeplususerpunchactions
                    WHERE checkintime IS NOT NULL 
                    AND checkouttime IS NOT NULL
                    AND checkindate = checkoutdate
                    AND checkouttime < '06:00:00'
                    AND checkintime >= '12:00:00'
                    AND checkindate >= %s
                    ORDER BY checkindate DESC
                """, (min_date,))
                early_checkouts = cursor.fetchall()
                
                if early_checkouts:
                    self.log(f"Found {len(early_checkouts)} early checkout anomalies (checkout before 6 AM after checkin after noon)")
                    
                    for early_checkout in early_checkouts:
                        employee_id = early_checkout['employeeid']
                        checkin_date = early_checkout['checkindate']
                        checkin_time = early_checkout['checkintime']
                        checkout_time = early_checkout['checkouttime']
                        
                        self.log(f"Analyzing early checkout anomaly: Employee {employee_id} on {checkin_date}")
                        self.log(f"  Checkin: {checkin_time}, Checkout: {checkout_time} (suspicious early checkout)")
                        
                        # This checkout belongs to the previous day's checkin, not current day
                        # Remove the checkout data from current day and wait for proper checkout
                        self.log(f"  Early checkout belongs to previous day - removing from current day")
                        
                        cursor.execute("""
                            UPDATE test_tymeplususerpunchactions
                            SET checkoutdate = NULL, checkouttime = NULL, checkoutdatetime = NULL,
                                punchoutid = NULL, logoutlatitude = NULL, logoutlocationid = NULL,
                                logoutlocationname = NULL, logoutlongitude = NULL,
                                punchoutreason = NULL, punchoutstatus = NULL
                            WHERE userpunchactionid = %s
                        """, (early_checkout['userpunchactionid'],))
                
                self.tgt_conn.commit()
                self.log("Time anomaly validation and cleanup completed successfully")
                
        except Exception as e:
            self.log(f"Error in validate_and_fix_time_anomalies: {e}")
            if self.tgt_conn:
                self.tgt_conn.rollback()

    def find_and_fix_mismatched_data(self):
        """Find and fix mismatched data before processing new records"""
        try:
            self.log("Starting data validation and fixing mismatched records...")
            
            # Set minimum date to August 1st, 2025
            min_date = datetime(2025, 8, 1).date()
            
            # Find orphaned checkouts (checkouts without corresponding checkins)
            with self.tgt_conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT userpunchactionid, employeeid, checkoutdate, checkouttime
                    FROM test_tymeplususerpunchactions
                    WHERE checkouttime IS NOT NULL 
                    AND checkintime IS NULL
                    AND checkoutdate >= %s
                """, (min_date,))
                orphaned_checkouts = cursor.fetchall()
                
                if orphaned_checkouts:
                    self.log(f"Found {len(orphaned_checkouts)} orphaned checkout records")
                    for orphan in orphaned_checkouts:
                        self.log(f"Removing orphaned checkout: Employee {orphan['employeeid']} on {orphan['checkoutdate']} at {orphan['checkouttime']}")
                        # Delete orphaned checkouts - they have no corresponding checkin
                        cursor.execute("""
                            DELETE FROM test_tymeplususerpunchactions
                            WHERE userpunchactionid = %s
                        """, (orphan['userpunchactionid'],))
                        self.log(f"Deleted orphaned checkout with no corresponding checkin")
                
                # Find incomplete records (checkins without checkouts) - leave them as is
                cursor.execute("""
                    SELECT userpunchactionid, employeeid, checkindate, checkintime
                    FROM test_tymeplususerpunchactions
                    WHERE checkintime IS NOT NULL 
                    AND checkouttime IS NULL
                    AND checkindate >= %s
                    AND checkindate < CURRENT_DATE - INTERVAL '1 day'
                """, (min_date,))
                incomplete_records = cursor.fetchall()
                
                if incomplete_records:
                    self.log(f"Found {len(incomplete_records)} incomplete records (checkins without checkouts) - these will remain incomplete")
                    for incomplete in incomplete_records:
                        self.log(f"Incomplete record: Employee {incomplete['employeeid']} checked in on {incomplete['checkindate']} but never checked out")
                
                # Find duplicate records
                cursor.execute("""
                    SELECT employeeid, checkindate, COUNT(*) as count
                    FROM test_tymeplususerpunchactions
                    WHERE checkintime IS NOT NULL
                    AND checkindate >= %s
                    GROUP BY employeeid, checkindate
                    HAVING COUNT(*) > 1
                """, (min_date,))
                duplicate_checkins = cursor.fetchall()
                
                if duplicate_checkins:
                    self.log(f"Found {len(duplicate_checkins)} duplicate checkin records")
                    for duplicate in duplicate_checkins:
                        self.log(f"Cleaning up duplicates for Employee {duplicate['employeeid']} on {duplicate['checkindate']}")
                        # Keep only the first record, delete others
                        cursor.execute("""
                            DELETE FROM test_tymeplususerpunchactions
                            WHERE employeeid = %s AND checkindate = %s AND checkintime IS NOT NULL
                            AND userpunchactionid NOT IN (
                                SELECT MIN(userpunchactionid)
                                FROM test_tymeplususerpunchactions
                                WHERE employeeid = %s AND checkindate = %s AND checkintime IS NOT NULL
                            )
                        """, (duplicate['employeeid'], duplicate['checkindate'], duplicate['employeeid'], duplicate['checkindate']))
                
                self.tgt_conn.commit()
                self.log("Data validation and fixing completed successfully")
                
        except Exception as e:
            self.log(f"Error in find_and_fix_mismatched_data: {e}")
            if self.tgt_conn:
                self.tgt_conn.rollback()

    def validate_checkout_time_logic(self, checkin_date, checkin_time: str, checkout_date, checkout_time: str) -> bool:
        """Validate that checkout time makes logical sense compared to checkin time"""
        try:
            checkin_dt = datetime.combine(checkin_date, self.parse_time_string(checkin_time))
            checkout_dt = datetime.combine(checkout_date, self.parse_time_string(checkout_time))
            
            # Checkout should be after checkin
            if checkout_dt <= checkin_dt:
                self.log(f"INVALID: Checkout time {checkout_time} on {checkout_date} is before or equal to checkin time {checkin_time} on {checkin_date}")
                return False
            
            # If same day, checkout should be at least 1 hour after checkin
            if checkin_date == checkout_date:
                time_diff = checkout_dt - checkin_dt
                if time_diff.total_seconds() < 3600:  # Less than 1 hour
                    self.log(f"SUSPICIOUS: Checkout only {time_diff} after checkin on same day")
                    return False
            
            return True
            
        except Exception as e:
            self.log(f"Error validating checkout time logic: {e}")
            return False

    def find_checkout_for_checkin(self, user_id: str, checkin_date, checkin_time: str) -> Optional[Dict]:
        """Find checkout for a checkin - search database for actual checkout data"""
        try:
            self.log(f"Looking for checkout for checkin: Employee {user_id} on {checkin_date} at {checkin_time}")
            
            current_date = datetime.now().date()
            
            # Search for actual checkout data in source database
            with self.src_conn.cursor(cursor_factory=RealDictCursor) as cursor:
                # First, check for checkout on the same day as checkin (after checkin time)
                cursor.execute("""
                    SELECT user_id, punch_date, punch_time, punch_type
                    FROM tymeplushr_attendance_logs
                    WHERE user_id = %s AND punch_type = 'checkout'
                    AND punch_date = %s
                    AND punch_time > %s
                    ORDER BY punch_time
                    LIMIT 1
                """, (user_id, checkin_date, checkin_time))
                same_day_checkout = cursor.fetchone()
                
                if same_day_checkout:
                    self.log(f"Found same-day checkout: {same_day_checkout['punch_time']}")
                    return same_day_checkout
            
            # Only check next day if the current day has passed (not today)
            if checkin_date < current_date:
                next_day = checkin_date + timedelta(days=1)
                with self.src_conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute("""
                        SELECT user_id, punch_date, punch_time, punch_type
                        FROM tymeplushr_attendance_logs
                        WHERE user_id = %s AND punch_type = 'checkout'
                        AND punch_date = %s
                        ORDER BY punch_time
                        LIMIT 1
                    """, (user_id, next_day))
                    next_day_checkout = cursor.fetchone()
                    
                    if next_day_checkout:
                        self.log(f"Found next-day checkout: {next_day_checkout['punch_date']} at {next_day_checkout['punch_time']}")
                        return next_day_checkout
            else:
                self.log(f"Checkin is on current day ({checkin_date}), not looking for next-day checkout yet")
            
            self.log(f"No actual checkout data found for Employee {user_id} on {checkin_date}")
            return None
            
        except Exception as e:
            self.log(f"Error finding checkout for checkin: {e}")
            return None

    def find_checkin_for_checkout(self, user_id: str, checkout_date, checkout_time: str) -> Optional[Dict]:
        """Find the appropriate checkin record for a checkout - handles cases where checkout is earlier than checkin"""
        try:
            self.log(f"Finding matching checkin for checkout: Employee {user_id} on {checkout_date} at {checkout_time}")
            
            # First, try to find checkin records from the same day as checkout
            search_dates = [checkout_date, checkout_date - timedelta(days=1), checkout_date - timedelta(days=2)]
            
            with self.tgt_conn.cursor(cursor_factory=RealDictCursor) as cursor:
                for search_date in search_dates:
                    cursor.execute("""
                        SELECT userpunchactionid, checkindate, checkintime, shiftstarttime, shiftendtime, overtimeid
                        FROM test_tymeplususerpunchactions
                        WHERE employeeid = %s AND checkindate = %s AND checkouttime IS NULL
                        ORDER BY checkindate DESC, checkintime DESC
                        LIMIT 1
                    """, (user_id, search_date))
                    checkin_record = cursor.fetchone()
                    if checkin_record:
                        # For cross-day scenarios, validate the chronological order
                        checkin_datetime = datetime.combine(
                            checkin_record['checkindate'], 
                            self.parse_time_string(checkin_record['checkintime'])
                        )
                        checkout_datetime = datetime.combine(
                            checkout_date, 
                            self.parse_time_string(checkout_time)
                        )
                        
                        # Ensure checkout is after checkin
                        if checkout_datetime > checkin_datetime:
                            self.log(f"Found matching checkin: {checkin_record['checkindate']} at {checkin_record['checkintime']}")
                            return checkin_record
                        else:
                            self.log(f"Checkout time {checkout_time} is before checkin time {checkin_record['checkintime']}, skipping")
                        
            self.log(f"No matching checkin found for Employee {user_id}")
            return None
        except Exception as e:
            self.log(f"Error finding matching checkin: {e}")
            return None

    def process_checkout(self, attendance_record: Dict, force_auto: bool = False) -> bool:
        """
        Process a checkout record from source database and update target database.
        If force_auto is True, always set punchoutstatus to 'auto' and punchoutreason to ''.
        """
        try:
            user_id = attendance_record['user_id']
            punch_date = attendance_record['punch_date']
            punch_time = attendance_record['punch_time']
            record_key = self.create_record_key(user_id, punch_date, punch_time, 'checkout')
            self.log(f"Processing checkout: Employee {user_id} on {punch_date} at {punch_time}")
            # Check if already processed to avoid duplicates
            if record_key in self.processed_records:
                self.log(f"Checkout already processed for {record_key}")
                return True
            # Get employee biometrics data for shift times and status
            biometrics = self.get_biometrics_data(user_id)
            if not biometrics:
                self.log(f"No biometrics data found for user {user_id}, skipping checkout")
                return False
            # Check if checkout record already exists in target database
            with self.tgt_conn.cursor() as cursor:
                cursor.execute("""
                    SELECT userpunchactionid FROM test_tymeplususerpunchactions
                    WHERE employeeid = %s AND checkoutdate = %s AND checkouttime IS NOT NULL
                """, (user_id, punch_date))
                if cursor.fetchone():
                    self.log(f"Checkout record already exists for Employee {user_id} on {punch_date}")
                    self.processed_records.add(record_key)
                    return True
            # Find the corresponding checkin record for this checkout
            checkin_record = self.find_checkin_for_checkout(user_id, punch_date, punch_time)
            if not checkin_record:
                self.log(f"No matching checkin found for checkout, skipping")
                return False
            # Validate that checkout time is logically after checkin time
            if not self.validate_checkout_time_logic(
                checkin_record['checkindate'], 
                checkin_record['checkintime'], 
                punch_date, 
                punch_time
            ):
                self.log(f"Time validation failed for checkout, skipping")
                return False
            # Calculate checkout status based on shift end time
            actual_shift_end = datetime.combine(
                checkin_record['checkindate'], 
                self.parse_time_string(str(biometrics['shiftendtime']))
            )
            punch_dt = datetime.combine(punch_date, self.parse_time_string(punch_time))
            # Determine checkout status and reason
            overtime_allowed = checkin_record.get('overtimeid', 0)
            if force_auto:
                punchout_status = 'auto'
                punchout_reason = ''
            elif punch_dt < actual_shift_end:
                punchout_status = 'earlycheckout'
                punchout_reason = 'earlycheckout'
            else:
                if overtime_allowed:
                    punchout_status = 'overtime'
                else:
                    punchout_status = 'manual'
                punchout_reason = ''
            self.log(f"Checkout status: {punchout_status}, reason: {punchout_reason}")
            # Update the checkin record with checkout information
            with self.tgt_conn.cursor() as cursor:
                cursor.execute("""
                    UPDATE test_tymeplususerpunchactions
                    SET checkouttime = %s,
                        checkoutdate = %s,
                        checkoutdatetime = %s,
                        punchoutid = %s,
                        logoutlatitude = %s,
                        logoutlocationid = %s,
                        logoutlocationname = %s,
                        logoutlongitude = %s,
                        punchoutreason = %s,
                        punchoutstatus = %s
                    WHERE userpunchactionid = %s
                """, (
                    punch_time,
                    punch_date,
                    punch_dt.isoformat() + 'Z',
                    DEFAULT_VALUES['punchoutid'],
                    DEFAULT_VALUES['logoutlatitude'],
                    DEFAULT_VALUES['logoutlocationid'],
                    DEFAULT_VALUES['logoutlocationname'],
                    DEFAULT_VALUES['logoutlongitude'],
                    punchout_reason,
                    punchout_status,
                    checkin_record['userpunchactionid']
                ))
                self.tgt_conn.commit()
            self.log(f"Successfully updated checkout record for Employee {user_id}")
            self.processed_records.add(record_key)
            return True
        except Exception as e:
            self.log(f"Error processing checkout for Employee {user_id}: {e}")
            if self.tgt_conn:
                self.tgt_conn.rollback()
            return False

    def process_checkin(self, attendance_record: Dict) -> bool:
        try:
            user_id = attendance_record['user_id']
            punch_date = attendance_record['punch_date']
            punch_time = attendance_record['punch_time']
            record_key = self.create_record_key(user_id, punch_date, punch_time, 'checkin')
            self.log(f"Processing checkin: Employee {user_id} on {punch_date} at {punch_time}")
            if record_key in self.processed_records:
                self.log(f"Checkin already processed for {record_key}")
                return True
            biometrics = self.get_biometrics_data(user_id)
            if not biometrics:
                self.log(f"No biometrics data found for user {user_id}, skipping checkin")
                return False
            with self.tgt_conn.cursor() as cursor:
                cursor.execute("""
                    SELECT userpunchactionid FROM test_tymeplususerpunchactions
                    WHERE employeeid = %s AND checkindate = %s
                """, (user_id, punch_date))
                if cursor.fetchone():
                    self.log(f"Checkin record already exists for Employee {user_id} on {punch_date}")
                    self.processed_records.add(record_key)
                    return True
            punch_status, punch_reason = self.calculate_punch_status(
                punch_time, str(biometrics['shiftstarttime']), True
            )
            self.log(f"Checkin status: {punch_status}, reason: {punch_reason}")
            overtime_flag = self.resolve_overtime_flag(biometrics)
            insert_data = {
                'clientid': biometrics['clientid'],
                'employeeid': biometrics['employeeid'],
                'fullname': biometrics['fullname'],
                'shiftendtime': str(biometrics['shiftendtime']),
                'shiftstarttime': str(biometrics['shiftstarttime']),
                'statusid': DEFAULT_VALUES['statusid'],
                'reportingmanager': biometrics['reportingmanager'],
                'checkindate': punch_date,
                'checkindatetime': punch_date,
                'checkintime': punch_time,
                'loginlatitude': DEFAULT_VALUES['loginlatitude'],
                'loginlocationid': DEFAULT_VALUES['loginlocationid'],
                'loginlocationname': DEFAULT_VALUES['loginlocationname'],
                'loginlongitude': DEFAULT_VALUES['loginlongitude'],
                'overtimeid': overtime_flag,
                'punchinid': DEFAULT_VALUES['punchinid'],
                'punchinreason': punch_reason,
                'punchinstatus': punch_status,
                'skipdistancechecking': DEFAULT_VALUES['skipdistancechecking'],
                'userid': biometrics['userid'],
                'departmentid': biometrics['basedepartmentid'],
                'departmentname': DEPARTMENT_MAP.get(str(biometrics['basedepartmentid']), 'Unknown')
            }
            with self.tgt_conn.cursor() as cursor:
                columns = ', '.join(insert_data.keys())
                placeholders = ', '.join(['%s'] * len(insert_data))
                query = f"""
                    INSERT INTO test_tymeplususerpunchactions ({columns})
                    VALUES ({placeholders})
                """
                cursor.execute(query, list(insert_data.values()))
                self.tgt_conn.commit()
            # After successful insert, update migration_status to 'Yes' in source
            with self.src_conn.cursor() as cursor:
                cursor.execute("""
                    UPDATE tymeplushr_attendance_logs
                    SET migration_status = 'Yes'
                    WHERE user_id = %s AND punch_date = %s AND punch_time = %s AND punch_type = %s
                """, (user_id, punch_date, punch_time, attendance_record.get('punch_type')))
                self.src_conn.commit()
            self.log(f"Successfully inserted checkin record for Employee {user_id}")
            self.processed_records.add(record_key)
            # After inserting checkin, look for corresponding checkout in source data
            current_date = datetime.now().date()
            if punch_date == current_date:
                self.log(f"Checkin is on current day, only looking for same-day checkout")
                checkout_record = self.find_checkout_for_checkin(user_id, punch_date, punch_time)
                if checkout_record:
                    self.log(f"Found corresponding checkout in source data, processing it now")
                    self.process_checkout(checkout_record)
                else:
                    self.log(f"No checkout data found in source for this checkin, leaving as incomplete record")
            else:
                # For past dates, look for both same-day and next-day checkouts
                checkout_record = self.find_checkout_for_checkin(user_id, punch_date, punch_time)
                if checkout_record:
                    self.log(f"Found corresponding checkout in source data, processing it now")
                    self.process_checkout(checkout_record)
                else:
                    # If no checkout found, check if next day 05:00 AM has passed, then auto-checkout
                    auto_checkout_date = punch_date + timedelta(days=1)
                    auto_checkout_time = "05:00:00"
                    now = datetime.now()
                    auto_checkout_datetime = datetime.combine(auto_checkout_date, datetime.strptime(auto_checkout_time, "%H:%M:%S").time())
                    if now > auto_checkout_datetime:
                        self.log(f"Auto-checking out Employee {user_id} on {auto_checkout_date} at {auto_checkout_time}")
                        auto_checkout_record = {
                            'user_id': user_id,
                            'punch_date': auto_checkout_date,
                            'punch_time': auto_checkout_time,
                            'punch_type': 'checkout'
                        }
                        self.process_checkout(auto_checkout_record, force_auto=True)
                    else:
                        self.log(f"No checkout data found in source for this checkin, leaving as incomplete record")
            return True
        except Exception as e:
            self.log(f"Error processing checkin for Employee {user_id}: {e}")
            if self.tgt_conn:
                self.tgt_conn.rollback()
            return False

    def process_attendance_by_date(self, target_date):
        """Process all attendance records for a specific date, handling cross-day scenarios"""
        try:
            self.log(f"Processing attendance for date: {target_date}")
            
            # Get records for the target date and next day to handle cross-day checkouts
            end_date = target_date + timedelta(days=1)
            all_records = self.get_attendance_logs_by_date_range(target_date, end_date)
            
            if not all_records:
                self.log(f"No attendance records found for {target_date}")
                return
            
            # Group records by user_id for proper chronological processing
            user_records = defaultdict(list)
            for record in all_records:
                user_records[record['user_id']].append(record)
            
            self.log(f"Processing {len(user_records)} users for date {target_date}")
            
            # Process each user's records chronologically
            for user_id, user_attendance in user_records.items():
                self.log(f"Processing user {user_id} with {len(user_attendance)} records")
                
                # Sort by date and time, with checkins processed before checkouts for same time
                user_attendance.sort(key=lambda x: (x['punch_date'], x['punch_time'], 
                                                  0 if x['punch_type'].lower() == 'checkin' else 1))
                
                # Track processed checkins to avoid duplicates
                processed_checkins = set()
                
                for record in user_attendance:
                    if record['punch_type'].lower() == 'checkin':
                        # Check if we already processed a checkin for this user on this date
                        checkin_key = f"{user_id}_{record['punch_date']}_checkin"
                        if checkin_key not in processed_checkins:
                            if self.process_checkin(record):
                                processed_checkins.add(checkin_key)
                    elif record['punch_type'].lower() == 'checkout':
                        # For standalone checkouts (not paired with checkins), process them
                        self.process_checkout(record)
                        
        except Exception as e:
            self.log(f"Error processing attendance for date {target_date}: {e}")

    def handle_cross_day_scenarios(self, start_date, end_date):
        """Specifically handle cross-day checkout scenarios"""
        try:
            self.log(f"Handling cross-day scenarios from {start_date} to {end_date}")
            
            # Get all records for the date range
            all_records = self.get_attendance_logs_by_date_range(start_date, end_date)
            
            # Group by user and find cross-day patterns
            user_patterns = defaultdict(list)
            for record in all_records:
                user_patterns[record['user_id']].append(record)
            
            cross_day_count = 0
            for user_id, records in user_patterns.items():
                # Sort records chronologically
                records.sort(key=lambda x: (x['punch_date'], x['punch_time']))
                
                # Find checkin-checkout pairs that span multiple days
                for i, record in enumerate(records):
                    if record['punch_type'].lower() == 'checkin':
                        # Look for corresponding checkout in next few days
                        checkin_date = record['punch_date']
                        checkin_time = record['punch_time']
                        
                        # Search for checkout in the next 3 days
                        for j in range(i + 1, min(i + 10, len(records))):
                            checkout_record = records[j]
                            if (checkout_record['punch_type'].lower() == 'checkout' and
                                checkout_record['punch_date'] > checkin_date):
                                # This is a cross-day scenario
                                checkout_date = checkout_record['punch_date']
                                checkout_time = checkout_record['punch_time']
                                
                                self.log(f"Found cross-day scenario: Employee {user_id} checked in on {checkin_date} at {checkin_time} and checked out on {checkout_date} at {checkout_time}")
                                
                                # Validate chronological order
                                checkin_dt = datetime.combine(checkin_date, self.parse_time_string(checkin_time))
                                checkout_dt = datetime.combine(checkout_date, self.parse_time_string(checkout_time))
                                
                                if checkout_dt > checkin_dt:
                                    # Process the checkin first
                                    if self.process_checkin(record):
                                        # Then process the checkout
                                        self.process_checkout(checkout_record)
                                        cross_day_count += 1
                                break
                                    
            self.log(f"Processed {cross_day_count} cross-day scenarios")
        except Exception as e:
            self.log(f"Error handling cross-day scenarios: {e}")

    def get_date_range_to_process(self):
        """Get the date range that needs to be processed"""
        try:
            # Set minimum date to August 1st, 2025
            min_date = datetime(2025, 8, 1).date()
            
            if self.last_sync_time:
                start_date = max(self.last_sync_time.date(), min_date)
            else:
                # If no last sync time, start from August 1st, 2025
                start_date = min_date
            
            end_date = datetime.now().date()
            
            # Ensure we don't process dates before August 1st, 2025
            if start_date < min_date:
                start_date = min_date
                self.log(f"Adjusted start date to minimum date: {start_date}")
            
            self.log(f"Processing date range: {start_date} to {end_date}")
            return start_date, end_date
        except:
            min_date = datetime(2025, 8, 1).date()
            return min_date, datetime.now().date()

    def validate_and_cleanup_duplicates(self):
        """Validate and clean up any duplicate records that might have been created"""
        try:
            self.log("Starting duplicate validation and cleanup...")
            
            with self.tgt_conn.cursor(cursor_factory=RealDictCursor) as cursor:
                # Find duplicate checkins for the same user on the same date
                cursor.execute("""
                    SELECT employeeid, checkindate, COUNT(*) as count
                    FROM test_tymeplususerpunchactions
                    WHERE checkouttime IS NULL
                    GROUP BY employeeid, checkindate
                    HAVING COUNT(*) > 1
                    ORDER BY checkindate DESC
                """)
                duplicate_checkins = cursor.fetchall()
                
                if duplicate_checkins:
                    self.log(f"Found {len(duplicate_checkins)} duplicate checkin records")
                    for duplicate in duplicate_checkins:
                        self.log(f"Cleaning up duplicate checkins for Employee {duplicate['employeeid']} on {duplicate['checkindate']}")
                        # Keep only the first checkin record for each user-date combination
                        cursor.execute("""
                            DELETE FROM test_tymeplususerpunchactions
                            WHERE employeeid = %s AND checkindate = %s AND checkouttime IS NULL
                            AND userpunchactionid NOT IN (
                                SELECT MIN(userpunchactionid)
                                FROM test_tymeplususerpunchactions
                                WHERE employeeid = %s AND checkindate = %s AND checkouttime IS NULL
                            )
                        """, (duplicate['employeeid'], duplicate['checkindate'], duplicate['employeeid'], duplicate['checkindate']))
                
                # Find duplicate checkouts for the same user on the same date
                cursor.execute("""
                    SELECT employeeid, checkoutdate, COUNT(*) as count
                    FROM test_tymeplususerpunchactions
                    WHERE checkouttime IS NOT NULL
                    GROUP BY employeeid, checkoutdate
                    HAVING COUNT(*) > 1
                    ORDER BY checkoutdate DESC
                """)
                duplicate_checkouts = cursor.fetchall()
                
                if duplicate_checkouts:
                    self.log(f"Found {len(duplicate_checkouts)} duplicate checkout records")
                    for duplicate in duplicate_checkouts:
                        self.log(f"Cleaning up duplicate checkouts for Employee {duplicate['employeeid']} on {duplicate['checkoutdate']}")
                        # Keep only the first checkout record for each user-date combination
                        cursor.execute("""
                            DELETE FROM test_tymeplususerpunchactions
                            WHERE employeeid = %s AND checkoutdate = %s AND checkouttime IS NOT NULL
                            AND userpunchactionid NOT IN (
                                SELECT MIN(userpunchactionid)
                                FROM test_tymeplususerpunchactions
                                WHERE employeeid = %s AND checkoutdate = %s AND checkouttime IS NOT NULL
                            )
                        """, (duplicate['employeeid'], duplicate['checkoutdate'], duplicate['employeeid'], duplicate['checkoutdate']))
                
                self.tgt_conn.commit()
                self.log("Duplicate validation and cleanup completed successfully")
                
        except Exception as e:
            self.log(f"Error in validate_and_cleanup_duplicates: {e}")
            if self.tgt_conn:
                self.tgt_conn.rollback()

    def process_auto_checkouts_for_incomplete_records(self):
        """
        Process auto-checkouts for incomplete records (checkins without checkouts).
        For each incomplete record, check if 05:00 AM of the next day has passed.
        If yes and no checkout data exists, perform auto-checkout at 05:00 AM.
        """
        try:
            self.log("Processing auto-checkouts for incomplete records...")
            auto_checkout_count = 0
            
            # Set minimum date to August 1st, 2025
            min_date = datetime(2025, 8, 1).date()
            current_date = datetime.now().date()
            
            # Find all incomplete records (checkins without checkouts)
            with self.tgt_conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT employeeid, checkindate, checkintime, userpunchactionid
                    FROM test_tymeplususerpunchactions
                    WHERE checkouttime IS NULL
                    AND checkindate >= %s
                    AND checkindate < %s
                    ORDER BY checkindate DESC
                """, (min_date, current_date))
                incomplete_records = cursor.fetchall()
            
            self.log(f"Found {len(incomplete_records)} incomplete records to process for auto-checkout")
            
            for incomplete_record in incomplete_records:
                employee_id = incomplete_record['employeeid']
                checkin_date = incomplete_record['checkindate']
                checkin_time = incomplete_record['checkintime']
                
                self.log(f"Processing auto-checkout for Employee {employee_id} who checked in on {checkin_date} at {checkin_time}")
                
                # Calculate the auto-checkout date and time (next day at 05:00 AM)
                auto_checkout_date = checkin_date + timedelta(days=1)
                auto_checkout_time = "05:00:00"
                
                # Check if 05:00 AM of the next day has already passed
                now = datetime.now()
                auto_checkout_datetime = datetime.combine(auto_checkout_date, datetime.strptime(auto_checkout_time, "%H:%M:%S").time())
                
                if now > auto_checkout_datetime:
                    self.log(f"05:00 AM of {auto_checkout_date} has passed, checking for actual checkout data...")
                    
                    # First, check if there's actual checkout data in source database
                    with self.src_conn.cursor(cursor_factory=RealDictCursor) as cursor:
                        cursor.execute("""
                            SELECT user_id, punch_date, punch_time, punch_type
                            FROM tymeplushr_attendance_logs
                            WHERE user_id = %s 
                              AND punch_type = 'checkout'
                              AND punch_date >= %s
                              AND punch_date <= %s
                              AND migration_status = 'No'
                            ORDER BY punch_date, punch_time
                            LIMIT 1
                        """, (employee_id, checkin_date, auto_checkout_date))
                        actual_checkout = cursor.fetchone()
                    
                    if actual_checkout:
                        self.log(f"Found actual checkout data for Employee {employee_id}: {actual_checkout['punch_date']} at {actual_checkout['punch_time']}")
                        # Process the actual checkout instead of auto-checkout
                        if self.process_checkout(actual_checkout):
                            # After processing, update migration_status to 'Yes'
                            with self.src_conn.cursor() as cursor2:
                                cursor2.execute("""
                                    UPDATE tymeplushr_attendance_logs
                                    SET migration_status = 'Yes'
                                    WHERE user_id = %s AND punch_date = %s AND punch_time = %s AND punch_type = %s
                                """, (actual_checkout['user_id'], actual_checkout['punch_date'], actual_checkout['punch_time'], actual_checkout['punch_type']))
                                self.src_conn.commit()
                            auto_checkout_count += 1
                    else:
                        self.log(f"No actual checkout data found, performing auto-checkout at 05:00 AM on {auto_checkout_date}")
                        
                        # Check if auto-checkout already exists
                        with self.tgt_conn.cursor() as cursor:
                            cursor.execute("""
                                SELECT userpunchactionid FROM test_tymeplususerpunchactions
                                WHERE employeeid = %s AND checkoutdate = %s AND checkouttime = %s
                            """, (employee_id, auto_checkout_date, auto_checkout_time))
                            if cursor.fetchone():
                                self.log(f"Auto-checkout already exists for Employee {employee_id} on {auto_checkout_date}")
                                continue
                        
                        # Perform auto-checkout
                        auto_checkout_record = {
                            'user_id': employee_id,
                            'punch_date': auto_checkout_date,
                            'punch_time': auto_checkout_time,
                            'punch_type': 'checkout'
                        }
                        
                        if self.process_checkout(auto_checkout_record, force_auto=True):
                            auto_checkout_count += 1
                            self.log(f"Successfully auto-checked out Employee {employee_id} at 05:00 AM on {auto_checkout_date}")
                else:
                    self.log(f"05:00 AM of {auto_checkout_date} has not passed yet, skipping auto-checkout for Employee {employee_id}")
            
            self.log(f"Auto-checkout processing completed. Processed {auto_checkout_count} records.")
            return auto_checkout_count
            
        except Exception as e:
            self.log(f"Error in process_auto_checkouts_for_incomplete_records: {e}")
            return 0

    def sync_incremental(self):
        """
        Main synchronization method that processes attendance data incrementally.
        
        This method performs the following steps:
        1. Validates and fixes existing data inconsistencies
        2. Handles cross-day attendance scenarios
        3. Processes attendance records date by date
        4. Processes auto-checkouts for incomplete records
        5. Cleans up any duplicate records
        6. Updates the last sync time
        """
        try:
            self.log("Starting incremental sync process...")
            self.ensure_connection()
            
            # Step 1: Find and fix any existing data inconsistencies
            self.find_and_fix_mismatched_data()
            
            # Step 2: Validate and fix time anomalies (checkout before checkin)
            self.validate_and_fix_time_anomalies()
            
            # Step 3: Get the date range to process
            start_date, end_date = self.get_date_range_to_process()
            
            # Step 4: Handle cross-day scenarios specifically (night shifts)
            self.handle_cross_day_scenarios(start_date, end_date)
            
            # Step 5: Process each date in chronological order
            current_date = start_date
            while current_date <= end_date:
                self.process_attendance_by_date(current_date)
                current_date += timedelta(days=1)
            
            # Step 6: Process auto-checkouts for incomplete records
            self.process_auto_checkouts_for_incomplete_records()
            
            # Step 7: Validate and clean up any duplicate records
            self.validate_and_cleanup_duplicates()
            
            # Update last sync time for next incremental run
            self.last_sync_time = datetime.now()
            self.log("Incremental sync process completed successfully")
        except Exception as e:
            self.log(f"Error in sync_incremental: {e}")

    def find_missing_checkouts(self) -> int:
        try:
            self.log("Finding missing checkouts...")
            missing_checkouts_updated = 0
            
            # Set minimum date to August 1st, 2025
            min_date = datetime(2025, 8, 1).date()
            current_date = datetime.now().date()
            
            with self.tgt_conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT employeeid, checkindate, userpunchactionid, shiftendtime
                    FROM test_tymeplususerpunchactions
                    WHERE checkouttime IS NULL
                    AND checkindate >= %s
                    AND checkindate < %s
                    ORDER BY checkindate DESC
                """, (min_date, current_date))
                missing_records = cursor.fetchall()
                
            self.log(f"Found {len(missing_records)} records with missing checkouts (excluding current day)")
            
            for missing_record in missing_records:
                employee_id = missing_record['employeeid']
                checkin_date = missing_record['checkindate']
                self.log(f"Looking for missing checkout for Employee {employee_id} on {checkin_date}")
                
                # Search for actual checkout data in source
                with self.src_conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute("""
                        SELECT user_id, punch_date, punch_time, punch_type
                        FROM tymeplushr_attendance_logs
                        WHERE user_id = %s 
                          AND punch_type = 'checkout'
                          AND punch_date >= %s
                          AND punch_date <= %s
                          AND migration_status = 'No'
                        ORDER BY punch_date, punch_time
                        LIMIT 1
                    """, (employee_id, checkin_date, checkin_date + timedelta(days=1), min_date))
                    checkout_record = cursor.fetchone()
                    if checkout_record:
                        self.log(f"Found missing checkout for Employee {employee_id}: {checkout_record['punch_date']} at {checkout_record['punch_time']}")
                        if self.process_checkout(checkout_record):
                            missing_checkouts_updated += 1
                    else:
                        self.log(f"No checkout data found in source for Employee {employee_id} - leaving as incomplete")
                        
            self.log(f"Updated {missing_checkouts_updated} missing checkouts with actual source data")
            return missing_checkouts_updated
        except Exception as e:
            self.log(f"Error finding missing checkouts: {e}")
            return 0

    def sync_missing_checkouts(self):
        try:
            self.log("Starting missing checkout sync...")
            self.ensure_connection()
            self.find_missing_checkouts()
            self.log("Missing checkout sync completed")
        except Exception as e:
            self.log(f"Error in sync_missing_checkouts: {e}")

def main():
    """
    Main entry point for the attendance processing application.
    
    This function:
    1. Creates an AttendanceProcessor instance
    2. Establishes database connections
    3. Runs the incremental sync process
    4. Syncs any missing checkouts
    5. Ensures proper cleanup of database connections
    """
    processor = AttendanceProcessor()
    try:
        processor.log("Starting attendance processing...")
        # Establish database connections
        processor.connect_databases()
        # Run the main incremental sync process
        processor.sync_incremental()
        # Find and sync any missing checkouts
        processor.sync_missing_checkouts()
        processor.log("Attendance processing completed successfully")
    except Exception as e:
        processor.log(f"Error in main: {e}")
    finally:
        # Always ensure connections are properly closed
        processor.close_connections()

if __name__ == "__main__":
    main()
