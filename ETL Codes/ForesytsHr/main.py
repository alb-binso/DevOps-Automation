import boto3
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
import json
from decimal import Decimal
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv("foremain.env", override=True)

# Configuration
DYNAMODB_REGION = os.getenv('AWS_REGION')
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')

POSTGRES_CONFIG = {
    'host': os.getenv('PG_HOST'),
    'database': os.getenv('PG_DATABASE'),
    'user': os.getenv('PG_USER'),
    'password': os.getenv('PG_PASSWORD'),
    'port': os.getenv('PG_PORT')
}

CLIENT_ID = os.getenv('CLIENT_ID', 'WASJKSP')

class DynamoDBToPostgresETL:
    def __init__(self):
        # Initialize DynamoDB client
        self.dynamodb = boto3.resource(
            'dynamodb',
            region_name=DYNAMODB_REGION,
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY
        )
        
        # Initialize PostgreSQL connection
        self.pg_conn = None
        self.pg_cursor = None
    
    def connect_postgres(self):
        """Establish PostgreSQL connection"""
        try:
            self.pg_conn = psycopg2.connect(**POSTGRES_CONFIG)
            self.pg_cursor = self.pg_conn.cursor()
            print("✓ Connected to PostgreSQL")
        except Exception as e:
            print(f"✗ PostgreSQL connection error: {e}")
            raise
    
    def decimal_to_float(self, obj):
        """Convert DynamoDB Decimal to float/int"""
        if isinstance(obj, Decimal):
            return int(obj) if obj % 1 == 0 else float(obj)
        elif isinstance(obj, dict):
            return {k: self.decimal_to_float(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self.decimal_to_float(i) for i in obj]
        return obj
    
    def create_tables(self):
        """Create PostgreSQL tables if they don't exist"""
        
        # Department table
        self.pg_cursor.execute("""
            CREATE TABLE IF NOT EXISTS foresytsdepartment (
                department_id VARCHAR(50) PRIMARY KEY,
                clientid VARCHAR(50),
                department_name VARCHAR(255),
                inserted_at TIMESTAMP,
                status_id INTEGER,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Leave Type table
        self.pg_cursor.execute("""
            CREATE TABLE IF NOT EXISTS foresytleavetype (
                leave_category_id VARCHAR(50) PRIMARY KEY,
                clientid VARCHAR(50),
                eligible VARCHAR(50),
                expiry_date VARCHAR(50),
                holiday VARCHAR(10),
                isSaturday VARCHAR(10),
                leave_category_name VARCHAR(255),
                leave_category_short_name VARCHAR(100),
                number_of_leaves VARCHAR(50),
                status_id VARCHAR(10),
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Holidays table
        self.pg_cursor.execute("""
            CREATE TABLE IF NOT EXISTS foresytsholidays (
                clientid VARCHAR(50),
                holiday_id VARCHAR(50),
                color VARCHAR(20),
                end_date DATE,
                holiday_name VARCHAR(255),
                inserted_at TIMESTAMP,
                location_id TEXT[],
                start_date DATE,
                status_id VARCHAR(10),
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (clientid, holiday_id)
            )
        """)
        
        # Employee Master table
        self.pg_cursor.execute("""
            CREATE TABLE IF NOT EXISTS foresytsemployee_master (
                userid VARCHAR(255) PRIMARY KEY,
                clientid VARCHAR(50),
                age INTEGER,
                basedepartmentid VARCHAR(50),
                baselocationid VARCHAR(50),
                companyname VARCHAR(255),
                employeeid VARCHAR(50),
                employementtype VARCHAR(100),
                enddate DATE,
                fullname VARCHAR(255),
                gender VARCHAR(20),
                hireddate DATE,
                jobtitle VARCHAR(255),
                maritalstatus VARCHAR(50),
                overtimestatus VARCHAR(10),
                profileurl TEXT,
                reportingmanager VARCHAR(255),
                managername VARCHAR(255),
                startdate DATE,
                statusid VARCHAR(10),
                usertype VARCHAR(50),
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (basedepartmentid) REFERENCES foresytsdepartment(department_id)
            )
        """)
        
        # Leave List table
        self.pg_cursor.execute("""
            CREATE TABLE IF NOT EXISTS foresytsleavelist (
                user_leave_id VARCHAR(50) PRIMARY KEY,
                userid VARCHAR(255),
                approved_rejected_by VARCHAR(255),
                approve_decline_status BOOLEAN,
                baselocationid VARCHAR(50),
                cancel_leave BOOLEAN,
                clientid VARCHAR(50),
                created_at TIMESTAMP,
                decline_reason TEXT,
                department_id VARCHAR(50),
                department_name VARCHAR(255),
                employeeid VARCHAR(50),
                end_date DATE,
                fullname VARCHAR(255),
                halfday VARCHAR(10),
                having_cancel_request VARCHAR(10),
                holiday VARCHAR(10),
                isSaturday VARCHAR(10),
                is_cancel_request BOOLEAN,
                leave_category_id VARCHAR(50),
                leave_category_name VARCHAR(255),
                new_end_date DATE,
                new_number_of_days VARCHAR(50),
                new_start_date DATE,
                number_of_days VARCHAR(50),
                reason TEXT,
                reporting_manager VARCHAR(255),
                reporting_manager_name VARCHAR(255),
                start_date DATE,
                status_id VARCHAR(10),
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (userid) REFERENCES foresytsemployee_master(userid),
                FOREIGN KEY (leave_category_id) REFERENCES foresytleavetype(leave_category_id)
            )
        """)
        
        # Absent List table
        self.pg_cursor.execute("""
            CREATE TABLE IF NOT EXISTS foresytsabsentlist (
                userAbsentActionId VARCHAR(50) PRIMARY KEY,
                clientId VARCHAR(50),
                baseDepartmentId VARCHAR(50),
                date DATE,
                employeeId VARCHAR(50),
                name VARCHAR(255),
                reportingmanager VARCHAR(255),
                statusId INTEGER,
                userId VARCHAR(255),
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (userId) REFERENCES foresytsemployee_master(userid)
            )
        """)
        
        self.pg_conn.commit()
        print("✓ Tables created/verified")
    
    def get_manager_name(self, manager_userid):
        """Get manager name from tymeplusUserAuth table"""
        if not manager_userid:
            return None
        
        try:
            table = self.dynamodb.Table('tymeplusUserAuth')
            response = table.get_item(
                Key={
                    'clientid': CLIENT_ID,
                    'userid': manager_userid
                }
            )
            
            if 'Item' in response:
                return response['Item'].get('fullname')
            return None
        except Exception as e:
            print(f"Warning: Could not fetch manager name for {manager_userid}: {e}")
            return None
    
    def migrate_departments(self):
        """Migrate department data from DynamoDB to PostgreSQL"""
        print("\n→ Migrating departments...")
        
        table = self.dynamodb.Table('tymeplusDepartmentMaster')
        
        # Scan with filter for clientid and status_id
        response = table.scan(
            FilterExpression='clientid = :cid AND status_id = :sid',
            ExpressionAttributeValues={
                ':cid': CLIENT_ID,
                ':sid': 1
            }
        )
        
        items = response.get('Items', [])
        
        # Handle pagination
        while 'LastEvaluatedKey' in response:
            response = table.scan(
                FilterExpression='clientid = :cid AND status_id = :sid',
                ExpressionAttributeValues={
                    ':cid': CLIENT_ID,
                    ':sid': 1
                },
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            items.extend(response.get('Items', []))
        
        for item in items:
            item = self.decimal_to_float(item)
            
            self.pg_cursor.execute("""
                INSERT INTO foresytsdepartment 
                (department_id, clientid, department_name, inserted_at, status_id, last_updated)
                VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (department_id) 
                DO UPDATE SET
                    clientid = EXCLUDED.clientid,
                    department_name = EXCLUDED.department_name,
                    inserted_at = EXCLUDED.inserted_at,
                    status_id = EXCLUDED.status_id,
                    last_updated = CURRENT_TIMESTAMP
            """, (
                item.get('department_id'),
                item.get('clientid'),
                item.get('department_name'),
                self.parse_timestamp(item.get('inserted_at')),
                item.get('status_id')
            ))
        
        self.pg_conn.commit()
        print(f"✓ Migrated {len(items)} departments")
    
    def migrate_leave_types(self):
        """Migrate leave type data from DynamoDB to PostgreSQL"""
        print("\n→ Migrating leave types...")
        
        table = self.dynamodb.Table('tymeplusLeaveCategories')
        
        # Scan with filter for clientid and status_id
        response = table.scan(
            FilterExpression='clientid = :cid AND status_id = :sid',
            ExpressionAttributeValues={
                ':cid': CLIENT_ID,
                ':sid': '1'
            }
        )
        
        items = response.get('Items', [])
        
        # Handle pagination
        while 'LastEvaluatedKey' in response:
            response = table.scan(
                FilterExpression='clientid = :cid AND status_id = :sid',
                ExpressionAttributeValues={
                    ':cid': CLIENT_ID,
                    ':sid': '1'
                },
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            items.extend(response.get('Items', []))
        
        for item in items:
            item = self.decimal_to_float(item)
            
            self.pg_cursor.execute("""
                INSERT INTO foresytleavetype 
                (leave_category_id, clientid, eligible, expiry_date, holiday, 
                 isSaturday, leave_category_name, leave_category_short_name, 
                 number_of_leaves, status_id, last_updated)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (leave_category_id) 
                DO UPDATE SET
                    clientid = EXCLUDED.clientid,
                    eligible = EXCLUDED.eligible,
                    expiry_date = EXCLUDED.expiry_date,
                    holiday = EXCLUDED.holiday,
                    isSaturday = EXCLUDED.isSaturday,
                    leave_category_name = EXCLUDED.leave_category_name,
                    leave_category_short_name = EXCLUDED.leave_category_short_name,
                    number_of_leaves = EXCLUDED.number_of_leaves,
                    status_id = EXCLUDED.status_id,
                    last_updated = CURRENT_TIMESTAMP
            """, (
                item.get('leave_category_id'),
                item.get('clientid'),
                item.get('eligible'),
                item.get('expiry_date'),
                item.get('holiday'),
                item.get('isSaturday'),
                item.get('leave_category_name'),
                item.get('leave_category_short_name'),
                item.get('number_of_leaves'),
                item.get('status_id')
            ))
        
        self.pg_conn.commit()
        print(f"✓ Migrated {len(items)} leave types")
    
    def migrate_holidays(self):
        """Migrate holiday data from DynamoDB to PostgreSQL"""
        print("\n→ Migrating holidays...")
        
        table = self.dynamodb.Table('tymeplusHolidayMaster')
        
        # Scan with filter for clientid and status_id
        response = table.scan(
            FilterExpression='clientid = :cid AND status_id = :sid',
            ExpressionAttributeValues={
                ':cid': CLIENT_ID,
                ':sid': '1'
            }
        )
        
        items = response.get('Items', [])
        
        # Handle pagination
        while 'LastEvaluatedKey' in response:
            response = table.scan(
                FilterExpression='clientid = :cid AND status_id = :sid',
                ExpressionAttributeValues={
                    ':cid': CLIENT_ID,
                    ':sid': '1'
                },
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            items.extend(response.get('Items', []))
        
        for item in items:
            item = self.decimal_to_float(item)
            
            # Convert location_id list to PostgreSQL array format
            location_ids = item.get('location_id', [])
            
            self.pg_cursor.execute("""
                INSERT INTO foresytsholidays 
                (clientid, holiday_id, color, end_date, holiday_name, 
                 inserted_at, location_id, start_date, status_id, last_updated)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (clientid, holiday_id) 
                DO UPDATE SET
                    color = EXCLUDED.color,
                    end_date = EXCLUDED.end_date,
                    holiday_name = EXCLUDED.holiday_name,
                    inserted_at = EXCLUDED.inserted_at,
                    location_id = EXCLUDED.location_id,
                    start_date = EXCLUDED.start_date,
                    status_id = EXCLUDED.status_id,
                    last_updated = CURRENT_TIMESTAMP
            """, (
                item.get('clientid'),
                item.get('holiday_id'),
                item.get('color'),
                self.parse_date(item.get('end_date')),
                item.get('holiday_name'),
                self.parse_timestamp(item.get('inserted_at')),
                location_ids,
                self.parse_date(item.get('start_date')),
                item.get('status_id')
            ))
        
        self.pg_conn.commit()
        print(f"✓ Migrated {len(items)} holidays")
    
    def migrate_employees(self):
        """Migrate employee data from DynamoDB to PostgreSQL"""
        print("\n→ Migrating employees...")
        
        table = self.dynamodb.Table('tymeplusUserAuth')
        
        # Scan with filter for clientid and statusid
        response = table.scan(
            FilterExpression='clientid = :cid AND statusid = :sid',
            ExpressionAttributeValues={
                ':cid': CLIENT_ID,
                ':sid': "1"
            }
        )
        
        items = response.get('Items', [])
        
        # Handle pagination
        while 'LastEvaluatedKey' in response:
            response = table.scan(
                FilterExpression='clientid = :cid AND statusid = :sid',
                ExpressionAttributeValues={
                    ':cid': CLIENT_ID,
                    ':sid': "1"
                },
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            items.extend(response.get('Items', []))
        
        for item in items:
            item = self.decimal_to_float(item)
            
            # Get manager name
            manager_name = self.get_manager_name(item.get('reportingmanager'))
            
            self.pg_cursor.execute("""
                INSERT INTO foresytsemployee_master 
                (userid, clientid, age, basedepartmentid, baselocationid, companyname, 
                 employeeid, employementtype, enddate, fullname, gender, hireddate, 
                 jobtitle, maritalstatus, overtimestatus, profileurl, reportingmanager, 
                 managername, startdate, statusid, usertype, last_updated)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (userid) 
                DO UPDATE SET
                    clientid = EXCLUDED.clientid,
                    age = EXCLUDED.age,
                    basedepartmentid = EXCLUDED.basedepartmentid,
                    baselocationid = EXCLUDED.baselocationid,
                    companyname = EXCLUDED.companyname,
                    employeeid = EXCLUDED.employeeid,
                    employementtype = EXCLUDED.employementtype,
                    enddate = EXCLUDED.enddate,
                    fullname = EXCLUDED.fullname,
                    gender = EXCLUDED.gender,
                    hireddate = EXCLUDED.hireddate,
                    jobtitle = EXCLUDED.jobtitle,
                    maritalstatus = EXCLUDED.maritalstatus,
                    overtimestatus = EXCLUDED.overtimestatus,
                    profileurl = EXCLUDED.profileurl,
                    reportingmanager = EXCLUDED.reportingmanager,
                    managername = EXCLUDED.managername,
                    startdate = EXCLUDED.startdate,
                    statusid = EXCLUDED.statusid,
                    usertype = EXCLUDED.usertype,
                    last_updated = CURRENT_TIMESTAMP
            """, (
                item.get('userid'),
                item.get('clientid'),
                item.get('age'),
                item.get('basedepartmentid'),
                item.get('baselocationid'),
                item.get('companyname'),
                item.get('employeeid'),
                item.get('employementtype'),
                self.parse_date(item.get('enddate')),
                item.get('fullname'),
                item.get('gender'),
                self.parse_date(item.get('hireddate')),
                item.get('jobtitle'),
                item.get('maritalstatus'),
                item.get('overtimestatus'),
                item.get('profileurl'),
                item.get('reportingmanager'),
                manager_name,
                self.parse_date(item.get('startdate')),
                item.get('statusid'),
                item.get('usertype')
            ))
        
        self.pg_conn.commit()
        print(f"✓ Migrated {len(items)} employees")
    
    def parse_date(self, date_value):
        """Parse date from various formats and return datetime.date object or None"""
        if not date_value or date_value == 'null' or date_value == '':
            return None
        
        try:
            # If it's already a date object, return it
            if isinstance(date_value, datetime):
                return date_value.date()
            
            # Try parsing ISO format date string (YYYY-MM-DD)
            if isinstance(date_value, str):
                # Remove any time component if present
                date_str = date_value.split('T')[0].split(' ')[0]
                return datetime.strptime(date_str, '%Y-%m-%d').date()
        except:
            return None
        
        return None
    
    def parse_timestamp(self, timestamp_value):
        """Parse timestamp from various formats and return datetime object or None"""
        if not timestamp_value or timestamp_value == 'null' or timestamp_value == '':
            return None
        
        try:
            if isinstance(timestamp_value, datetime):
                return timestamp_value
            
            if isinstance(timestamp_value, str):
                # Handle ISO format with Z
                return datetime.fromisoformat(timestamp_value.replace('Z', '+00:00'))
        except:
            return None
        
        return None
    
    def migrate_leaves(self):
        """Migrate leave data from DynamoDB to PostgreSQL"""
        print("\n→ Migrating leaves...")
        
        table = self.dynamodb.Table('tymeplusUserLeaves')
        
        # Scan with filter for clientid
        response = table.scan(
            FilterExpression='clientid = :cid',
            ExpressionAttributeValues={
                ':cid': CLIENT_ID
            }
        )
        
        items = response.get('Items', [])
        
        # Handle pagination
        while 'LastEvaluatedKey' in response:
            response = table.scan(
                FilterExpression='clientid = :cid',
                ExpressionAttributeValues={
                    ':cid': CLIENT_ID
                },
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            items.extend(response.get('Items', []))
        
        migrated_count = 0
        skipped_count = 0
        
        for item in items:
            item = self.decimal_to_float(item)
            
            # Check if the employee exists in the employee_master table
            userid = item.get('userid')
            self.pg_cursor.execute(
                "SELECT 1 FROM foresytsemployee_master WHERE userid = %s",
                (userid,)
            )
            employee_exists = self.pg_cursor.fetchone() is not None
            
            if not employee_exists:
                skipped_count += 1
                print(f"  ⚠ Skipping leave record for non-existent/inactive employee: {userid}")
                continue
            
            self.pg_cursor.execute("""
                INSERT INTO foresytsleavelist 
                (user_leave_id, userid, approved_rejected_by, approve_decline_status, 
                 baselocationid, cancel_leave, clientid, created_at, decline_reason, 
                 department_id, department_name, employeeid, end_date, fullname, 
                 halfday, having_cancel_request, holiday, isSaturday, is_cancel_request, 
                 leave_category_id, leave_category_name, new_end_date, new_number_of_days, 
                 new_start_date, number_of_days, reason, reporting_manager, 
                 reporting_manager_name, start_date, status_id, last_updated)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (user_leave_id) 
                DO UPDATE SET
                    userid = EXCLUDED.userid,
                    approved_rejected_by = EXCLUDED.approved_rejected_by,
                    approve_decline_status = EXCLUDED.approve_decline_status,
                    baselocationid = EXCLUDED.baselocationid,
                    cancel_leave = EXCLUDED.cancel_leave,
                    clientid = EXCLUDED.clientid,
                    created_at = EXCLUDED.created_at,
                    decline_reason = EXCLUDED.decline_reason,
                    department_id = EXCLUDED.department_id,
                    department_name = EXCLUDED.department_name,
                    employeeid = EXCLUDED.employeeid,
                    end_date = EXCLUDED.end_date,
                    fullname = EXCLUDED.fullname,
                    halfday = EXCLUDED.halfday,
                    having_cancel_request = EXCLUDED.having_cancel_request,
                    holiday = EXCLUDED.holiday,
                    isSaturday = EXCLUDED.isSaturday,
                    is_cancel_request = EXCLUDED.is_cancel_request,
                    leave_category_id = EXCLUDED.leave_category_id,
                    leave_category_name = EXCLUDED.leave_category_name,
                    new_end_date = EXCLUDED.new_end_date,
                    new_number_of_days = EXCLUDED.new_number_of_days,
                    new_start_date = EXCLUDED.new_start_date,
                    number_of_days = EXCLUDED.number_of_days,
                    reason = EXCLUDED.reason,
                    reporting_manager = EXCLUDED.reporting_manager,
                    reporting_manager_name = EXCLUDED.reporting_manager_name,
                    start_date = EXCLUDED.start_date,
                    status_id = EXCLUDED.status_id,
                    last_updated = CURRENT_TIMESTAMP
            """, (
                item.get('user_leave_id'),
                item.get('userid'),
                item.get('approved_rejected_by'),
                item.get('approve_decline_status'),
                item.get('baselocationid'),
                item.get('cancel_leave'),
                item.get('clientid'),
                self.parse_timestamp(item.get('created_at')),
                item.get('decline_reason'),
                item.get('department_id'),
                item.get('department_name'),
                item.get('employeeid'),
                self.parse_date(item.get('end_date')),
                item.get('fullname'),
                item.get('halfday'),
                item.get('having_cancel_request'),
                item.get('holiday'),
                item.get('isSaturday'),
                item.get('is_cancel_request'),
                item.get('leave_category_id'),
                item.get('leave_category_name'),
                self.parse_date(item.get('new_end_date')),
                item.get('new_number_of_days'),
                self.parse_date(item.get('new_start_date')),
                item.get('number_of_days'),
                item.get('reason'),
                item.get('reporting_manager'),
                item.get('reporting_manager_name'),
                self.parse_date(item.get('start_date')),
                item.get('status_id')
            ))
            migrated_count += 1
        
        self.pg_conn.commit()
        print(f"✓ Migrated {migrated_count} leave records")
        if skipped_count > 0:
            print(f"  ℹ Skipped {skipped_count} records for inactive/non-existent employees")
    
    def migrate_absents(self):
        """Migrate absent data from DynamoDB to PostgreSQL"""
        print("\n→ Migrating absents...")
        
        table = self.dynamodb.Table('tymeplusUserAbsentList')
        
        # Scan with filter for clientId
        response = table.scan(
            FilterExpression='clientId = :cid',
            ExpressionAttributeValues={
                ':cid': CLIENT_ID
            }
        )
        
        items = response.get('Items', [])
        
        # Handle pagination
        while 'LastEvaluatedKey' in response:
            response = table.scan(
                FilterExpression='clientId = :cid',
                ExpressionAttributeValues={
                    ':cid': CLIENT_ID
                },
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            items.extend(response.get('Items', []))
        
        migrated_count = 0
        skipped_count = 0
        
        for item in items:
            item = self.decimal_to_float(item)
            
            # Check if the employee exists in the employee_master table
            userid = item.get('userId')
            self.pg_cursor.execute(
                "SELECT 1 FROM foresytsemployee_master WHERE userid = %s",
                (userid,)
            )
            employee_exists = self.pg_cursor.fetchone() is not None
            
            if not employee_exists:
                skipped_count += 1
                print(f"  ⚠ Skipping absent record for non-existent/inactive employee: {userid}")
                continue
            
            self.pg_cursor.execute("""
                INSERT INTO foresytsabsentlist 
                (userAbsentActionId, clientId, baseDepartmentId, date, employeeId, 
                 name, reportingmanager, statusId, userId, last_updated)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (userAbsentActionId) 
                DO UPDATE SET
                    clientId = EXCLUDED.clientId,
                    baseDepartmentId = EXCLUDED.baseDepartmentId,
                    date = EXCLUDED.date,
                    employeeId = EXCLUDED.employeeId,
                    name = EXCLUDED.name,
                    reportingmanager = EXCLUDED.reportingmanager,
                    statusId = EXCLUDED.statusId,
                    userId = EXCLUDED.userId,
                    last_updated = CURRENT_TIMESTAMP
            """, (
                item.get('userAbsentActionId'),
                item.get('clientId'),
                item.get('baseDepartmentId'),
                self.parse_date(item.get('date')),
                item.get('employeeId'),
                item.get('name'),
                item.get('reportingmanager'),
                item.get('statusId'),
                item.get('userId')
            ))
            migrated_count += 1
        
        self.pg_conn.commit()
        print(f"✓ Migrated {migrated_count} absent records")
        if skipped_count > 0:
            print(f"  ℹ Skipped {skipped_count} records for inactive/non-existent employees")
    
    def run(self):
        """Execute the complete ETL process"""
        try:
            print("=" * 60)
            print("DynamoDB to PostgreSQL ETL Process")
            print("=" * 60)
            
            self.connect_postgres()
            self.create_tables()
            
            # Migrate in order (respecting foreign key dependencies)
            self.migrate_departments()
            self.migrate_leave_types()
            self.migrate_holidays()
            self.migrate_employees()
            self.migrate_leaves()
            self.migrate_absents()
            
            print("\n" + "=" * 60)
            print("✓ ETL Process Completed Successfully!")
            print("=" * 60)
            
        except Exception as e:
            print(f"\n✗ ETL Process Failed: {e}")
            if self.pg_conn:
                self.pg_conn.rollback()
            raise
        
        finally:
            if self.pg_cursor:
                self.pg_cursor.close()
            if self.pg_conn:
                self.pg_conn.close()
                print("\n✓ Database connections closed")

if __name__ == "__main__":
    etl = DynamoDBToPostgresETL()
    etl.run()