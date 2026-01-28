import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import psycopg2
from psycopg2.extras import RealDictCursor
import boto3
from boto3.dynamodb.conditions import Key, Attr

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Database Configurations
SRC_DB = {
    'dbname': '',
    'user': '',
    'password': '',
    'host': '',
    'port': 5432
}

TGT_DB = {
    'dbname': '',
    'user': '',
    'password': '',
    'host': '',
    'port': 5432
}

DYNAMODB_CONFIG = {
    'region_name': '',
    'client_id': '',
    'aws_access_key_id': '',
    'aws_secret_access_key': ''
}

# Constants
DEPARTMENT_MAP = {
    '1': 'Reception',
    '2': 'OPD1',
    '3': 'OPD2',
    '4': 'Consultant',
    '5': 'OT',
    '6': 'Insurance',
    '7': 'Optical',
    '8': 'House Keeping',
    '9': 'Driver',
    '10': 'Accounts',
}

DEFAULT_VALUES = {
    'loginlatitude': 10.7904833,
    'loginlocationid': 2,
    'loginlocationname': 'Kenrail Towers',
    'loginlongitude': 78.7046725,
    'logoutlatitude': 10.7904833,
    'logoutlocationid': 2,
    'logoutlocationname': 'Kenrail Towers',
    'logoutlongitude': 78.7046725,
    'punchinid': 1,
    'punchoutid': 0,
    'skipdistancechecking': False,
    'statusid': 1
}

DEVICE_ID = 'GED7243200199'
CLIENT_ID = 'WEKOQEJ'
AUTO_CHECKOUT_TIME = '21:00'


class BiometricETLPipeline:
    def __init__(self):
        """Initialize database and DynamoDB connections"""
        try:
            # Source DB Connection (for attendance logs)
            self.source_conn = psycopg2.connect(**SRC_DB)
            logger.info("Connected to source database successfully")
            
            # Target DB Connection (for biometrics_master and target table)
            self.target_conn = psycopg2.connect(**TGT_DB)
            logger.info("Connected to target database successfully")
            
            # DynamoDB Connection
            self.dynamodb = boto3.resource(
                'dynamodb',
                region_name=DYNAMODB_CONFIG['region_name'],
                aws_access_key_id=DYNAMODB_CONFIG['aws_access_key_id'],
                aws_secret_access_key=DYNAMODB_CONFIG['aws_secret_access_key']
            )
            
            self.shift_allocation_table = self.dynamodb.Table('tymeplushrShiftAllocation')
            self.shift_management_table = self.dynamodb.Table('tymeplushrShiftManagement')
            logger.info("Connected to DynamoDB tables successfully")
            
        except Exception as e:
            logger.error(f"Error initializing connections: {e}")
            raise

    def __del__(self):
        """Close database connections"""
        try:
            if hasattr(self, 'source_conn') and self.source_conn:
                self.source_conn.close()
                logger.info("Source database connection closed")
            if hasattr(self, 'target_conn') and self.target_conn:
                self.target_conn.close()
                logger.info("Target database connection closed")
        except Exception as e:
            logger.error(f"Error closing connections: {e}")

    def get_current_date(self) -> str:
        """Get current date in YYYY-MM-DD format"""
        return datetime.now().strftime('%Y-%m-%d')

    def fetch_attendance_logs(self, punch_date: str) -> List[Dict]:
        """Fetch attendance logs for the given date"""
        query = """
            SELECT user_id, punch_date, punch_time, punch_type, migration_status
            FROM public.tymeplushr_attendance_logs
            WHERE device_id = %s AND punch_date = %s
            ORDER BY user_id, punch_time
        """
        
        try:
            with self.source_conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, (DEVICE_ID, punch_date))
                results = cursor.fetchall()
                logger.info(f"Fetched {len(results)} attendance logs for {punch_date}")
                return [dict(row) for row in results]
        except Exception as e:
            logger.error(f"Error fetching attendance logs: {e}")
            raise

    def get_biometrics_master_data(self, employee_id: str) -> Optional[Dict]:
        """Fetch employee data from biometrics_master"""
        query = """
            SELECT statusid, reportingmanager, overtimestatus, fullname, 
                   employeeid, basedepartmentid, userid, clientid
            FROM public.biometrics_master
            WHERE employeeid = %s
        """
        
        try:
            with self.target_conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, (employee_id,))
                result = cursor.fetchone()
                
                if result:
                    return dict(result)
                else:
                    logger.warning(f"No biometrics data found for employee {employee_id}")
                    return None
        except Exception as e:
            logger.error(f"Error fetching biometrics master data: {e}")
            return None

    def get_shift_allocation(self, userid: str, current_date: str) -> Optional[Dict]:
        """Get shift allocation for user on specific date"""
        try:
            # Try with 'userId' as partition key (capital I)
            try:
                response = self.shift_allocation_table.query(
                    KeyConditionExpression=Key('userId').eq(userid),
                    FilterExpression=Attr('clientId').eq(CLIENT_ID)
                )
            except Exception as e1:
                # If that fails, try scanning with userid filter
                logger.info(f"Trying scan operation for user {userid}")
                response = self.shift_allocation_table.scan(
                    FilterExpression=Attr('userid').eq(userid) | Attr('userId').eq(userid)
                )
            
            items = response.get('Items', [])
            
            # Filter by clientId if not already filtered
            items = [item for item in items if item.get('clientId') == CLIENT_ID]
            
            for item in items:
                dates = item.get('dates', [])
                for date_entry in dates:
                    if date_entry.get('date') == current_date:
                        shift_id = date_entry.get('shift_id')
                        if shift_id:
                            logger.info(f"Found shift_id {shift_id} for user {userid} on {current_date}")
                            return date_entry
            
            logger.warning(f"No shift allocation found for user {userid} on {current_date}")
            return None
            
        except Exception as e:
            logger.error(f"Error fetching shift allocation: {e}")
            return None

    def get_shift_management(self, shift_id: int) -> Optional[Dict]:
        """Get shift timings from shift management"""
        try:
            response = self.shift_management_table.get_item(
                Key={
                    'clientId': CLIENT_ID,
                    'shift_id': shift_id
                }
            )
            
            item = response.get('Item')
            if item:
                logger.info(f"Found shift management data for shift_id {shift_id}")
                return item
            else:
                logger.warning(f"No shift management data found for shift_id {shift_id}")
                return None
                
        except Exception as e:
            logger.error(f"Error fetching shift management: {e}")
            return None

    def calculate_punch_status(self, punch_time: str, shift_start_time: str, 
                             punch_type: str, shift_end_time: str = None, 
                             overtime_allowed: bool = False) -> tuple:
        """Calculate punch in/out status and reason"""
        try:
            # Parse times - handle both 'HH:MM:SS' and 'HH:MM' formats
            def parse_time(time_str: str):
                """Parse time string to time object"""
                time_str = time_str.strip()
                try:
                    # Try HH:MM:SS format first
                    return datetime.strptime(time_str, '%H:%M:%S').time()
                except ValueError:
                    # Try HH:MM format
                    return datetime.strptime(time_str, '%H:%M').time()
            
            punch_time_obj = parse_time(punch_time)
            
            if punch_type.lower() == 'checkin':
                shift_start_time_obj = parse_time(shift_start_time)
                
                # Checkin status logic (NO GRACE TIME):
                # Example: shift_start = 09:00
                # - If checkintime <= 09:00:00 = ontime
                # - If checkintime > 09:00:00 = late (09:01, 09:02, etc. are all late)
                logger.debug(f"Checkin comparison: punch={punch_time_obj.strftime('%H:%M:%S')}, shift_start={shift_start_time_obj.strftime('%H:%M:%S')}")
                
                if punch_time_obj <= shift_start_time_obj:
                    logger.info(f"Checkin status: ONTIME (punch: {punch_time} <= shift start: {shift_start_time})")
                    return 'ontime', 'ontime'
                else:
                    logger.info(f"Checkin status: LATE (punch: {punch_time} > shift start: {shift_start_time})")
                    return 'late', 'late'
            
            elif punch_type.lower() == 'checkout':
                if not shift_end_time:
                    logger.error("shift_end_time is required for checkout status calculation")
                    return 'manual', 'manual'
                    
                shift_end_time_obj = parse_time(shift_end_time)
                
                # Convert overtime_allowed to boolean
                overtime_flag = False
                if isinstance(overtime_allowed, str):
                    val = overtime_allowed.strip().lower()
                    if val.isdigit():
                        overtime_flag = bool(int(val))
                    else:
                        overtime_flag = val in ('yes', 'true', '1')
                elif isinstance(overtime_allowed, (int, float)):
                    overtime_flag = bool(overtime_allowed)
                else:
                    overtime_flag = bool(overtime_allowed)
                
                # Checkout status logic:
                # - If checkouttime < shiftendtime: earlycheckout (regardless of overtimeid 0 or 1)
                # - If checkouttime >= shiftendtime AND overtimeid == 0: manual
                # - If checkouttime >= shiftendtime AND overtimeid == 1: overtime
                if punch_time_obj < shift_end_time_obj:
                    logger.info(f"Checkout status: EARLY CHECKOUT (punch: {punch_time} < shift end: {shift_end_time})")
                    return 'earlycheckout', 'earlycheckout'
                elif punch_time_obj >= shift_end_time_obj:
                    if overtime_flag:
                        logger.info(f"Checkout status: OVERTIME (punch: {punch_time} >= shift end: {shift_end_time}, OT allowed: {overtime_flag})")
                        return 'overtime', 'overtime'
                    else:
                        logger.info(f"Checkout status: MANUAL (punch: {punch_time} >= shift end: {shift_end_time}, OT not allowed: {overtime_flag})")
                        return 'manual', 'manual'
                    
        except Exception as e:
            logger.error(f"Error calculating punch status for {punch_type}: {e}")
            logger.error(f"  punch_time: {punch_time}, shift_start_time: {shift_start_time}, shift_end_time: {shift_end_time}")
            import traceback
            logger.error(traceback.format_exc())
            # Return appropriate default based on punch type
            if punch_type.lower() == 'checkin':
                return 'late', 'late'
            else:
                return 'manual', 'manual'

    def check_existing_checkin(self, userid: str, checkin_date: str) -> Optional[Dict]:
        """Check if checkin already exists for the user on the given date"""
        query = """
            SELECT * FROM public.tymeplususerpunchactions
            WHERE userid = %s AND checkindate = %s
            LIMIT 1
        """
        
        try:
            with self.target_conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, (userid, checkin_date))
                result = cursor.fetchone()
                return dict(result) if result else None
        except Exception as e:
            logger.error(f"Error checking existing checkin: {e}")
            return None

    def insert_checkin(self, bio_data: Dict, shift_data: Dict, log_data: Dict) -> bool:
        """Insert checkin record"""
        try:
            punch_status, punch_reason = self.calculate_punch_status(
                log_data['punch_time'],
                shift_data['starttime'],
                'checkin'
            )
            
            checkin_datetime = f"{log_data['punch_date']}T{log_data['punch_time']}.000Z"
            
            insert_query = """
                INSERT INTO public.tymeplususerpunchactions (
                    clientid, employeeid, fullname, shiftendtime, shiftstarttime,
                    statusid, reportingmanager, checkindate, checkindatetime, checkintime,
                    loginlatitude, loginlocationid, loginlocationname, loginlongitude,
                    overtimeid, punchinid, punchoutid, punchinreason, punchinstatus,
                    skipdistancechecking, userid, departmentid, departmentname
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
            """
            
            overtime_id = 1 if bio_data.get('overtimestatus', '').lower() == 'yes' else 0
            department_name = DEPARTMENT_MAP.get(str(bio_data.get('basedepartmentid', '')), '')
            
            values = (
                bio_data['clientid'],
                bio_data['employeeid'],
                bio_data['fullname'],
                shift_data['endtime'],
                shift_data['starttime'],
                DEFAULT_VALUES['statusid'],
                bio_data.get('reportingmanager', ''),
                log_data['punch_date'],
                checkin_datetime,
                log_data['punch_time'],
                DEFAULT_VALUES['loginlatitude'],
                DEFAULT_VALUES['loginlocationid'],
                DEFAULT_VALUES['loginlocationname'],
                DEFAULT_VALUES['loginlongitude'],
                overtime_id,
                DEFAULT_VALUES['punchinid'],
                0,
                punch_reason,
                punch_status,
                DEFAULT_VALUES['skipdistancechecking'],
                bio_data['userid'],
                bio_data.get('basedepartmentid'),
                department_name
            )
            
            with self.target_conn.cursor() as cursor:
                cursor.execute(insert_query, values)
                self.target_conn.commit()
                
            logger.info(f"✓ Successfully inserted checkin for {bio_data['userid']} ({bio_data['fullname']}) on {log_data['punch_date']} at {log_data['punch_time']} - Status: {punch_status}")
            return True
            
        except Exception as e:
            self.target_conn.rollback()
            logger.error(f"✗ Error inserting checkin: {e}")
            return False

    def update_checkout(self, userid: str, checkin_date: str, log_data: Dict, 
                       shift_data: Dict, overtime_allowed: bool) -> bool:
        """Update existing checkin record with checkout data"""
        try:
            punch_status, punch_reason = self.calculate_punch_status(
                log_data['punch_time'],
                shift_data['starttime'],
                'checkout',
                shift_data['endtime'],
                overtime_allowed
            )
            
            checkout_datetime = f"{log_data['punch_date']}T{log_data['punch_time']}.000Z"
            
            update_query = """
                UPDATE public.tymeplususerpunchactions
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
                WHERE userid = %s AND checkindate = %s
            """
            
            values = (
                log_data['punch_time'],
                log_data['punch_date'],
                checkout_datetime,
                2,
                DEFAULT_VALUES['logoutlatitude'],
                DEFAULT_VALUES['logoutlocationid'],
                DEFAULT_VALUES['logoutlocationname'],
                DEFAULT_VALUES['logoutlongitude'],
                punch_reason,
                punch_status,
                userid,
                checkin_date
            )
            
            with self.target_conn.cursor() as cursor:
                cursor.execute(update_query, values)
                self.target_conn.commit()
                
            logger.info(f"✓ Successfully updated checkout for {userid} on {checkin_date} at {log_data['punch_time']} - Status: {punch_status}")
            return True
            
        except Exception as e:
            self.target_conn.rollback()
            logger.error(f"✗ Error updating checkout: {e}")
            return False

    def auto_checkout(self, userid: str, checkin_date: str) -> bool:
        """Perform auto checkout for users who forgot to checkout"""
        try:
            checkout_datetime = f"{checkin_date}T{AUTO_CHECKOUT_TIME}:00.000Z"
            
            update_query = """
                UPDATE public.tymeplususerpunchactions
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
                WHERE userid = %s AND checkindate = %s AND checkouttime IS NULL
            """
            
            values = (
                AUTO_CHECKOUT_TIME,
                checkin_date,
                checkout_datetime,
                2,
                DEFAULT_VALUES['logoutlatitude'],
                DEFAULT_VALUES['logoutlocationid'],
                DEFAULT_VALUES['logoutlocationname'],
                DEFAULT_VALUES['logoutlongitude'],
                'auto checkout',
                'auto checkout',
                userid,
                checkin_date
            )
            
            with self.target_conn.cursor() as cursor:
                cursor.execute(update_query, values)
                rows_affected = cursor.rowcount
                self.target_conn.commit()
                
                if rows_affected > 0:
                    logger.info(f"⏰ Auto checkout performed for {userid} on {checkin_date}")
                    return True
                    
            return False
            
        except Exception as e:
            self.target_conn.rollback()
            logger.error(f"✗ Error performing auto checkout: {e}")
            return False

    def update_migration_status(self, employee_id: str, punch_date: str, 
                               punch_type: str, status: str) -> bool:
        """Update migration status in attendance logs"""
        try:
            update_query = """
                UPDATE public.tymeplushr_attendance_logs
                SET migration_status = %s
                WHERE user_id = %s AND punch_date = %s AND punch_type = %s
            """
            
            with self.source_conn.cursor() as cursor:
                cursor.execute(update_query, (status, employee_id, punch_date, punch_type))
                self.source_conn.commit()
                
            logger.info(f"Updated migration status to '{status}' for {employee_id} on {punch_date} - {punch_type}")
            return True
            
        except Exception as e:
            self.source_conn.rollback()
            logger.error(f"Error updating migration status: {e}")
            return False

    def process_user_attendance(self, user_logs: List[Dict], current_date: str):
        """Process attendance logs for a single user"""
        if not user_logs:
            return
        
        employee_id = user_logs[0]['user_id']
        logger.info(f"\n{'='*60}")
        logger.info(f"Processing attendance for employee: {employee_id}")
        logger.info(f"{'='*60}")
        
        # Get biometrics master data
        bio_data = self.get_biometrics_master_data(employee_id)
        if not bio_data:
            logger.warning(f"⊘ Skipping {employee_id} - No biometrics data found")
            return
        
        # Check if user is active
        if str(bio_data.get('statusid')) != '1':
            logger.info(f"⊘ Skipping {employee_id} - User not active (statusid: {bio_data.get('statusid')})")
            return
        
        userid = bio_data['userid']
        logger.info(f"User ID: {userid} | Full Name: {bio_data['fullname']}")
        
        # Get shift allocation
        shift_allocation = self.get_shift_allocation(userid, current_date)
        if not shift_allocation:
            logger.warning(f"⊘ Skipping {employee_id} - No shift allocation for {current_date}")
            return
        
        shift_id = shift_allocation.get('shift_id')
        if not shift_id:
            logger.warning(f"⊘ Skipping {employee_id} - No shift_id found")
            return
        
        logger.info(f"Shift: {shift_allocation.get('shifttitle')} (ID: {shift_id})")
        
        # Get shift management data
        shift_data = self.get_shift_management(shift_id)
        if not shift_data:
            logger.warning(f"⊘ Skipping {employee_id} - No shift management data")
            return
        
        logger.info(f"Shift Timings: {shift_data['starttime']} - {shift_data['endtime']}")
        
        # Separate checkin and checkout logs
        checkin_logs = [log for log in user_logs if log['punch_type'].lower() == 'checkin']
        checkout_logs = [log for log in user_logs if log['punch_type'].lower() == 'checkout']
        
        logger.info(f"Found {len(checkin_logs)} checkin(s) and {len(checkout_logs)} checkout(s)")
        
        # Process checkin (only first one)
        if checkin_logs:
            first_checkin = checkin_logs[0]
            
            # Check if checkin already exists
            existing_checkin = self.check_existing_checkin(userid, current_date)
            
            if not existing_checkin:
                # Insert new checkin
                success = self.insert_checkin(bio_data, shift_data, first_checkin)
                if success:
                    self.update_migration_status(employee_id, current_date, 'checkin', 'Yes')
                    
                    # Mark additional checkins as duplicate
                    if len(checkin_logs) > 1:
                        logger.info(f"Marking {len(checkin_logs) - 1} additional checkin(s) as duplicate")
                        for extra_checkin in checkin_logs[1:]:
                            self.update_migration_status(employee_id, current_date, 'checkin', 'Duplicate')
            else:
                logger.info(f"⊘ Checkin already exists for {userid} on {current_date}")
                # Mark all as duplicate
                for checkin in checkin_logs:
                    self.update_migration_status(employee_id, current_date, 'checkin', 'Duplicate')
        
        # Process checkout (only if checkin exists)
        existing_record = self.check_existing_checkin(userid, current_date)
        
        if existing_record and checkout_logs:
            first_checkout = checkout_logs[0]
            
            # Only update if checkout doesn't exist
            if not existing_record.get('checkouttime'):
                success = self.update_checkout(
                    userid, 
                    current_date, 
                    first_checkout, 
                    shift_data,
                    bool(existing_record.get('overtimeid', 0))
                )
                if success:
                    self.update_migration_status(employee_id, current_date, 'checkout', 'Yes')
                    
                    # Mark additional checkouts as duplicate
                    if len(checkout_logs) > 1:
                        logger.info(f"Marking {len(checkout_logs) - 1} additional checkout(s) as duplicate")
                        for extra_checkout in checkout_logs[1:]:
                            self.update_migration_status(employee_id, current_date, 'checkout', 'Duplicate')
            else:
                logger.info(f"⊘ Checkout already exists for {userid} on {current_date}")
                # Mark all as duplicate
                for checkout in checkout_logs:
                    self.update_migration_status(employee_id, current_date, 'checkout', 'Duplicate')
        elif checkout_logs and not existing_record:
            logger.warning(f"⊘ Skipping checkout for {userid} - No checkin record found")

    def process_auto_checkouts(self, current_date: str):
        """Process auto checkouts for users who didn't checkout"""
        current_time = datetime.now().time()
        auto_checkout_dt = datetime.strptime(AUTO_CHECKOUT_TIME, '%H:%M').time()
        
        # Only run auto checkout after 21:00
        if current_time < auto_checkout_dt:
            logger.info(f"\n⏰ Auto checkout time not reached yet (current: {current_time.strftime('%H:%M')}, required: {AUTO_CHECKOUT_TIME})")
            return
        
        logger.info(f"\n{'='*60}")
        logger.info(f"Processing Auto Checkouts")
        logger.info(f"{'='*60}")
        
        query = """
            SELECT userid, checkindate, fullname
            FROM public.tymeplususerpunchactions
            WHERE checkindate = %s AND checkouttime IS NULL
        """
        
        try:
            with self.target_conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, (current_date,))
                missing_checkouts = cursor.fetchall()
                
                if missing_checkouts:
                    logger.info(f"Found {len(missing_checkouts)} user(s) with missing checkouts")
                    
                    for record in missing_checkouts:
                        logger.info(f"Processing auto checkout for: {record['userid']} ({record.get('fullname', 'N/A')})")
                        self.auto_checkout(record['userid'], record['checkindate'])
                else:
                    logger.info("No users with missing checkouts found")
                    
        except Exception as e:
            logger.error(f"Error processing auto checkouts: {e}")

    def run(self):
        """Main ETL pipeline execution"""
        try:
            current_date = self.get_current_date()
            logger.info(f"\n{'#'*60}")
            logger.info(f"# BIOMETRICS ETL PIPELINE STARTED")
            logger.info(f"# Date: {current_date}")
            logger.info(f"# Time: {datetime.now().strftime('%H:%M:%S')}")
            logger.info(f"{'#'*60}\n")
            
            # Fetch all attendance logs for current date
            attendance_logs = self.fetch_attendance_logs(current_date)
            
            if not attendance_logs:
                logger.info(f"No attendance logs found for {current_date}")
                logger.info("\nETL pipeline completed - No data to process")
                return
            
            # Group logs by user
            user_logs_map = {}
            for log in attendance_logs:
                user_id = log['user_id']
                if user_id not in user_logs_map:
                    user_logs_map[user_id] = []
                user_logs_map[user_id].append(log)
            
            logger.info(f"Processing attendance for {len(user_logs_map)} unique user(s)\n")
            
            # Process each user's attendance
            processed_count = 0
            error_count = 0
            
            for user_id, logs in user_logs_map.items():
                try:
                    self.process_user_attendance(logs, current_date)
                    processed_count += 1
                except Exception as e:
                    logger.error(f"✗ Error processing user {user_id}: {e}")
                    error_count += 1
                    continue
            
            # Process auto checkouts
            self.process_auto_checkouts(current_date)
            
            logger.info(f"\n{'#'*60}")
            logger.info(f"# ETL PIPELINE COMPLETED")
            logger.info(f"# Users Processed: {processed_count}")
            logger.info(f"# Errors: {error_count}")
            logger.info(f"# Completion Time: {datetime.now().strftime('%H:%M:%S')}")
            logger.info(f"{'#'*60}\n")
            
        except Exception as e:
            logger.error(f"\n{'!'*60}")
            logger.error(f"! ETL PIPELINE FAILED")
            logger.error(f"! Error: {e}")
            logger.error(f"{'!'*60}\n")
            raise


def main():
    """Entry point for the ETL pipeline"""
    try:
        pipeline = BiometricETLPipeline()
        pipeline.run()
    except Exception as e:
        logger.error(f"Fatal error in ETL pipeline: {e}")
        raise


if __name__ == "__main__":
    main()