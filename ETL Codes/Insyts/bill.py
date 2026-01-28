import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
import sys

class BillingETL:
    def __init__(self, db1_config, db2_config, dest_config):
        """
        Initialize ETL with database configurations
        db1_config: Config for tymeplususerauth, insytssupervisorlist, insytsbillings
        db2_config: Config for insytsalltimesheets
        dest_config: Config for destination database
        """
        self.db1_config = db1_config
        self.db2_config = db2_config
        self.dest_config = dest_config
        
    def get_connection(self, config):
        """Create database connection"""
        try:
            conn = psycopg2.connect(
                host=config['host'],
                port=config['port'],
                database=config['database'],
                user=config['user'],
                password=config['password']
            )
            return conn
        except Exception as e:
            print(f"❌ Error connecting to database: {e}")
            sys.exit(1)
    
    def format_date(self, date_str):
        """Convert 2021-09-13T08:09:11.000Z to 13-Aug-2021"""
        if not date_str:
            return None
        try:
            dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            return dt.strftime('%d-%b-%Y')
        except:
            return None
    
    def create_destination_table(self):
        """Create the destination table if it doesn't exist"""
        print("📋 Creating destination table if not exists...")
        
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS insytsallbills (
            userid VARCHAR(255),
            fullname VARCHAR(255),
            engagement VARCHAR(500),
            engagementid VARCHAR(255),
            partner VARCHAR(255),
            billingid VARCHAR(255) PRIMARY KEY,
            status VARCHAR(100),
            clientcode VARCHAR(255),
            clientname VARCHAR(500),
            billingoffice VARCHAR(255),
            managingoffice VARCHAR(255),
            internalbilling VARCHAR(50),
            startdate VARCHAR(20),
            approver VARCHAR(255),
            approveddate VARCHAR(20),
            declineddate VARCHAR(20),
            serviceline VARCHAR(255),
            subserviceline VARCHAR(255),
            manager VARCHAR(255),
            active_status INTEGER,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE INDEX IF NOT EXISTS idx_billingid ON insytsallbills(billingid);
        CREATE INDEX IF NOT EXISTS idx_userid ON insytsallbills(userid);
        CREATE INDEX IF NOT EXISTS idx_fullname ON insytsallbills(fullname);
        """
        
        dest_conn = self.get_connection(self.dest_config)
        try:
            with dest_conn.cursor() as cur:
                cur.execute(create_table_sql)
                dest_conn.commit()
                print("✅ Destination table created/verified successfully")
        except Exception as e:
            print(f"❌ Error creating destination table: {e}")
            dest_conn.rollback()
            raise
        finally:
            dest_conn.close()
    
    def get_user_fullname(self, userid, user_dict):
        """Get fullname from user dictionary"""
        user_info = user_dict.get(userid, {})
        if isinstance(user_info, dict):
            return user_info.get('fullname', '')
        return ''
    
    def get_user_status(self, userid, user_dict):
        """Get statusid from user dictionary"""
        user_info = user_dict.get(userid, {})
        if isinstance(user_info, dict):
            return user_info.get('statusid', 0)
        return 0
    
    def extract_source_data(self):
        """Extract data from source databases"""
        print("\n🔍 Extracting data from source databases...")
        
        # Connect to DB1 (billings and users)
        db1_conn = self.get_connection(self.db1_config)
        
        # Extract user auth data
        print("  → Fetching user authentication data...")
        with db1_conn.cursor() as cur:
            cur.execute("SELECT userid, fullname, statusid FROM tymeplususerauth")
            user_data = cur.fetchall()
            user_dict = {row[0]: {'fullname': row[1], 'statusid': row[2]} for row in user_data}
        print(f"  ✓ Loaded {len(user_dict)} users")
        
        # Extract billing data
        print("  → Fetching billing data...")
        with db1_conn.cursor() as cur:
            billing_query = """
            SELECT 
                userid,
                engagementname,
                engagementid,
                engagementpartner,
                billingid,
                statusname,
                clientcode,
                clientname,
                billingoffice,
                managingoffice,
                internalbilling,
                createdat,
                billingapprovedby,
                billingapprovedbyfinance,
                billingapproveddate,
                billingapproveddatefinance,
                billingdeclineddate,
                billingdeclineddatefinance
            FROM insytsbillings
            """
            cur.execute(billing_query)
            billing_data = cur.fetchall()
        print(f"  ✓ Loaded {len(billing_data)} billing records")
        
        db1_conn.close()
        
        # Connect to DB2 (timesheets)
        db2_conn = self.get_connection(self.db2_config)
        
        # Extract timesheet data
        print("  → Fetching timesheet data...")
        with db2_conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT 
                    fullname, 
                    serviceline, 
                    subserviceline, 
                    manager 
                FROM insytsalltimesheets
            """)
            timesheet_data = cur.fetchall()
            timesheet_dict = {row[0]: {
                'serviceline': row[1],
                'subserviceline': row[2],
                'manager': row[3]
            } for row in timesheet_data}
        print(f"  ✓ Loaded {len(timesheet_dict)} timesheet records")
        
        db2_conn.close()
        
        return user_dict, billing_data, timesheet_dict
    
    def transform_data(self, user_dict, billing_data, timesheet_dict):
        """Transform billing data according to business rules"""
        print("\n🔄 Transforming data...")
        
        transformed_records = []
        
        for row in billing_data:
            (userid, engagement, engagementid, engagementpartner, billingid, 
             status, clientcode, clientname, billingoffice, managingoffice,
             internalbilling, createdat, billingapprovedby, billingapprovedbyfinance,
             billingapproveddate, billingapproveddatefinance, billingdeclineddate,
             billingdeclineddatefinance) = row
            
            # Get fullname
            fullname = self.get_user_fullname(userid, user_dict)
            
            # Get active status
            active_status = self.get_user_status(userid, user_dict)
            
            # Get partner name
            partner = self.get_user_fullname(engagementpartner, user_dict)
            
            # Add prefix to managing office
            managingoffice_formatted = f"BDO EA {managingoffice}" if managingoffice else ""
            
            # Format start date
            startdate = self.format_date(createdat)
            
            # Determine approver based on status
            approver = ""
            if status == 'Approved by Partner':
                approver = self.get_user_fullname(billingapprovedby, user_dict)
            elif status == 'Approved by Finance':
                approver = self.get_user_fullname(billingapprovedbyfinance, user_dict)
            
            # Determine approved date
            approveddate = ""
            if status == 'Approved by Partner':
                approveddate = self.format_date(billingapproveddate)
            elif status == 'Approved by Finance':
                approveddate = self.format_date(billingapproveddatefinance)
            
            # Determine declined date
            declineddate = ""
            if status == 'Declined by Partner':
                declineddate = self.format_date(billingdeclineddate)
            elif status == 'Declined by Finance':
                declineddate = self.format_date(billingdeclineddatefinance)
            
            # Get timesheet info based on fullname
            timesheet_info = timesheet_dict.get(fullname, {})
            serviceline = timesheet_info.get('serviceline', '')
            subserviceline = timesheet_info.get('subserviceline', '')
            manager = timesheet_info.get('manager', '')
            
            transformed_records.append((
                userid, fullname, engagement, engagementid, partner, billingid,
                status, clientcode, clientname, billingoffice, managingoffice_formatted,
                internalbilling, startdate, approver, approveddate, declineddate,
                serviceline, subserviceline, manager, active_status
            ))
        
        print(f"✅ Transformed {len(transformed_records)} records")
        return transformed_records
    
    def load_data(self, transformed_records):
        """Load data into destination with upsert logic"""
        print("\n💾 Loading data to destination...")
        
        dest_conn = self.get_connection(self.dest_config)
        
        try:
            with dest_conn.cursor() as cur:
                # Upsert query
                upsert_query = """
                INSERT INTO insytsallbills (
                    userid, fullname, engagement, engagementid, partner, billingid,
                    status, clientcode, clientname, billingoffice, managingoffice,
                    internalbilling, startdate, approver, approveddate, declineddate,
                    serviceline, subserviceline, manager, active_status, last_updated
                ) VALUES %s
                ON CONFLICT (billingid) 
                DO UPDATE SET
                    userid = EXCLUDED.userid,
                    fullname = EXCLUDED.fullname,
                    engagement = EXCLUDED.engagement,
                    engagementid = EXCLUDED.engagementid,
                    partner = EXCLUDED.partner,
                    status = EXCLUDED.status,
                    clientcode = EXCLUDED.clientcode,
                    clientname = EXCLUDED.clientname,
                    billingoffice = EXCLUDED.billingoffice,
                    managingoffice = EXCLUDED.managingoffice,
                    internalbilling = EXCLUDED.internalbilling,
                    startdate = EXCLUDED.startdate,
                    approver = EXCLUDED.approver,
                    approveddate = EXCLUDED.approveddate,
                    declineddate = EXCLUDED.declineddate,
                    serviceline = EXCLUDED.serviceline,
                    subserviceline = EXCLUDED.subserviceline,
                    manager = EXCLUDED.manager,
                    active_status = EXCLUDED.active_status,
                    last_updated = CURRENT_TIMESTAMP
                """
                
                # Add timestamp to each record
                records_with_timestamp = [
                    record + (datetime.now(),) for record in transformed_records
                ]
                
                execute_values(cur, upsert_query, records_with_timestamp)
                dest_conn.commit()
                
                # Get stats
                cur.execute("SELECT COUNT(*) FROM insytsallbills")
                total_count = cur.fetchone()[0]
                
                print(f"✅ Data loaded successfully")
                print(f"  → Total records in destination: {total_count}")
                
        except Exception as e:
            print(f"❌ Error loading data: {e}")
            dest_conn.rollback()
            raise
        finally:
            dest_conn.close()
    
    def run(self):
        """Execute the full ETL pipeline"""
        print("=" * 60)
        print("🚀 Starting Billing ETL Pipeline")
        print("=" * 60)
        start_time = datetime.now()
        
        try:
            # Step 1: Create destination table
            self.create_destination_table()
            
            # Step 2: Extract
            user_dict, billing_data, timesheet_dict = self.extract_source_data()
            
            # Step 3: Transform
            transformed_records = self.transform_data(user_dict, billing_data, timesheet_dict)
            
            # Step 4: Load
            self.load_data(transformed_records)
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            print("\n" + "=" * 60)
            print(f"✅ ETL Pipeline completed successfully in {duration:.2f} seconds")
            print("=" * 60)
            
        except Exception as e:
            print("\n" + "=" * 60)
            print(f"❌ ETL Pipeline failed: {e}")
            print("=" * 60)
            sys.exit(1)

# Example usage
if __name__ == "__main__":
    # Database 1 configuration (billings and users)
    db1_config = {
        'host': 'b-hosting-server',
        'database': 'db-name',
        'user': 'db-username',
        'password': 'db-password',
        'port': 5432
    }
    
    # Database 2 configuration (timesheets)
    db2_config = {
        'host': 'hosting-server',
        'database': 'db-name',
        'user': 'db-username',
        'password': 'db-password',
        'port': 5432
    }
    
    # Destination database configuration
    dest_config = {
        'host': 'tymeplushr-dev.c2k74vrdhhrw.ap-south-1.rds.amazonaws.com',
        'database': 'Tractask',
        'user': 'tymeplushradmin',
        'password': 'qpRjNflkH3msHLy71SIu',
        'port': 5432
    }
    
    # Run ETL
    etl = BillingETL(db1_config, db2_config, dest_config)
    etl.run()