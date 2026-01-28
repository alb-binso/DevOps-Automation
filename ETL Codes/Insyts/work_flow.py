import psycopg2
from psycopg2.extras import execute_values, RealDictCursor
import json
from datetime import datetime
import logging
from typing import Dict, List, Optional, Any
import sys
from decimal import Decimal

# Configure logging with UTF-8 encoding for Windows
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_pipeline.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Set console to UTF-8 for Windows
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')


class DatabaseConfig:
    """Database configuration class"""
    def __init__(self, host: str, port: int, database: str, user: str, password: str):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password


class EngagementETL:
    """ETL Pipeline for Full Engagement List"""
    
    def __init__(self, source_db: DatabaseConfig, dest_db: DatabaseConfig):
        self.source_db = source_db  # Source DB
        self.dest_db = dest_db
        self.conn_source = None
        self.conn_dest = None
        
    def connect_databases(self):
        """Establish database connections"""
        try:
            logger.info("Connecting to source database...")
            self.conn_source = psycopg2.connect(
                host=self.source_db.host,
                port=self.source_db.port,
                database=self.source_db.database,
                user=self.source_db.user,
                password=self.source_db.password
            )
            logger.info("[OK] Connected to source database")
            
            logger.info("Connecting to destination database...")
            self.conn_dest = psycopg2.connect(
                host=self.dest_db.host,
                port=self.dest_db.port,
                database=self.dest_db.database,
                user=self.dest_db.user,
                password=self.dest_db.password
            )
            logger.info("[OK] Connected to destination database")
            
        except Exception as e:
            logger.error(f"[ERROR] Database connection failed: {str(e)}")
            raise
    
    def close_connections(self):
        """Close all database connections"""
        for conn in [self.conn_source, self.conn_dest]:
            if conn:
                conn.close()
        logger.info("All database connections closed")
    
    def parse_json_field(self, field: str, key: str, default: Any = None) -> Any:
        """Safely parse JSON field"""
        try:
            if not field:
                return default
            data = json.loads(field) if isinstance(field, str) else field
            return data.get(key, default)
        except (json.JSONDecodeError, AttributeError, TypeError):
            return default
    
    def format_date(self, date_str: str) -> Optional[str]:
        """Format date to DD-MMM-YYYY for PowerBI with flexible parsing"""
        if not date_str:
            return None
        s = str(date_str).strip()
        # Quick ISO path
        if 'T' in s:
            try:
                dt = datetime.fromisoformat(s.replace('Z', '+00:00'))
                return dt.strftime('%d-%b-%Y')
            except Exception as e:
                logger.warning(f"Date formatting error for {date_str}: {e}")
                return None

        candidates = [
            '%d-%m-%Y', '%d/%m/%Y',
            '%Y-%m-%d', '%Y/%m/%d',
            '%d-%b-%Y', '%d-%b-%y',
            '%d-%B-%Y', '%d-%B-%y',
            '%m/%d/%Y', '%m-%d-%Y'
        ]
        for fmt in candidates:
            try:
                dt = datetime.strptime(s, fmt)
                return dt.strftime('%d-%b-%Y')
            except Exception:
                continue

        logger.warning(f"Date formatting error for {date_str}: no matching format")
        return None
    
    def get_subserviceline(self, subserviceline_str: str) -> str:
        """Parse and format subserviceline"""
        try:
            if not subserviceline_str:
                return None
            services = json.loads(subserviceline_str) if isinstance(subserviceline_str, str) else subserviceline_str
            if isinstance(services, list):
                if len(services) == 0:
                    return None
                elif len(services) == 1:
                    return services[0]
                else:
                    return "Multi-SubServices"
            return None
        except Exception as e:
            logger.warning(f"Subserviceline parsing error: {e}")
            return None
    
    def get_resource_plan_table(self, managing_office: str) -> str:
        """Determine which resource plan table to use based on managing office"""
        office_mapping = {
            'BDO EA Burundi': 'insytsburundiresourceplan',
            'BDO EA Ethiopia': 'insytsethiopiaresourceplan',
            'BDO EA Head Office': 'insytsheadofficeresourceplan',
            'BDO EA Rwanda': 'insytsrwandaresourceplan',
            'BDO EA Tanzania': 'insytstanzaniaresourceplan',
            'BDO EA Uganda': 'insytsugandaresourceplan'
        }
        return office_mapping.get(managing_office, 'insytsresourceplan')
    
    def calculate_approved_hours(self, timesheet_data: List[Dict], engagement_id: str) -> float:
        """Calculate total approved hours for an engagement"""
        total_hours = 0.0
        try:
            for entry in timesheet_data:
                if entry.get('engagement') == engagement_id and entry.get('status') == 'approved':
                    sum_val = entry.get('sum', 0)
                    total_hours += float(sum_val) if sum_val else 0.0
        except Exception as e:
            logger.warning(f"Error calculating hours for {engagement_id}: {e}")
        return total_hours

    def build_timesheet_hours(self, timesheets_raw: List[Dict]) -> Dict[str, float]:
        """
        Pre-aggregate approved hours per engagement across all timesheets to avoid
        per-engagement JSON parsing later.
        """
        engagement_hours: Dict[str, float] = {}
        for row in timesheets_raw:
            ts_blob = row.get('timesheet')
            if not ts_blob:
                continue
            try:
                ts_list = json.loads(ts_blob) if isinstance(ts_blob, str) else ts_blob
                if not isinstance(ts_list, list):
                    continue
                for entry in ts_list:
                    if not isinstance(entry, dict):
                        continue
                    if entry.get('status') != 'approved':
                        continue
                    eng_id = entry.get('engagement')
                    sum_val = entry.get('sum', 0)
                    if eng_id:
                        engagement_hours[eng_id] = engagement_hours.get(eng_id, 0.0) + (float(sum_val) if sum_val else 0.0)
            except Exception as e:
                logger.warning(f"Error parsing timesheet row: {e}")
        return engagement_hours

    def preload_resource_plan_summaries(self, cursor, tables: List[str]) -> Dict[str, Dict[str, Dict[str, float]]]:
        """
        Preload resource plan aggregates (rate, mandays) per engagement for each table
        to minimize per-engagement queries.
        """
        summaries: Dict[str, Dict[str, Dict[str, float]]] = {}
        for table in tables:
            try:
                cursor.execute(
                    f"""
                    SELECT engagementid,
                           COALESCE(SUM(CAST(resourceextrate AS NUMERIC)), 0) AS total_rate,
                           COALESCE(SUM(CAST(personday AS NUMERIC)), 0) AS total_mandays
                    FROM {table}
                    GROUP BY engagementid
                    """
                )
                rows = cursor.fetchall()
                summaries[table] = {
                    row['engagementid']: {'rate': float(row['total_rate']), 'mandays': float(row['total_mandays'])}
                    for row in rows
                }
            except Exception as e:
                logger.warning(f"Error preloading resource plan summaries from {table}: {e}")
                try:
                    cursor.connection.rollback()
                except Exception:
                    pass
                summaries[table] = {}
        return summaries
    
    # Deprecated per-engagement resource lookups (kept for reference); now using preloaded summaries
    def get_resource_ext_rate(self, cursor, engagement_id: str, resource_table: str) -> float:
        return 0.0
    
    def get_mandays(self, cursor, engagement_id: str, resource_table: str) -> float:
        return 0.0
    
    def create_destination_table(self):
        """Create the destination table if it doesn't exist"""
        logger.info("Creating destination table if not exists...")
        
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS fullengagementlist (
            engagementid VARCHAR(100) PRIMARY KEY,
            engagementname TEXT,
            createdate VARCHAR(20),
            closedate VARCHAR(20),
            partner TEXT,
            personincharge TEXT,
            managingoffice VARCHAR(100),
            serviceline VARCHAR(100),
            clientid VARCHAR(100),
            clientname TEXT,
            fullname TEXT,
            jobcloser VARCHAR(50),
            billingid VARCHAR(100),
            bills NUMERIC(18, 2),
            billscreateddate VARCHAR(20),
            billscloseddate VARCHAR(20),
            declineddate VARCHAR(20),
            conversionrate NUMERIC(18, 10),
            usdamount NUMERIC(18, 2),
            chargeout NUMERIC(18, 2),
            actualchargeout NUMERIC(18, 2),
            mandays NUMERIC(18, 2),
            disbursement NUMERIC(18, 2),
            status INTEGER,
            subserviceline VARCHAR(100),
            manager TEXT,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE INDEX IF NOT EXISTS idx_fullengagement_engagementid ON fullengagementlist(engagementid);
        CREATE INDEX IF NOT EXISTS idx_fullengagement_clientid ON fullengagementlist(clientid);
        CREATE INDEX IF NOT EXISTS idx_fullengagement_managingoffice ON fullengagementlist(managingoffice);
        """
        
        try:
            with self.conn_dest.cursor() as cursor:
                cursor.execute(create_table_sql)
                self.conn_dest.commit()
            logger.info("[OK] Destination table created/verified")
        except Exception as e:
            logger.error(f"[ERROR] Error creating destination table: {e}")
            raise
    
    def fetch_source_data(self):
        """Fetch data from source database"""
        logger.info("Fetching source data...")
        
        try:
            with self.conn_source.cursor(cursor_factory=RealDictCursor) as cursor:
                # Fetch main engagement data
                logger.info("Fetching engagement data...")
                cursor.execute("""
                    SELECT * FROM insytsengagement
                """)
                engagements = cursor.fetchall()
                logger.info(f"[OK] Fetched {len(engagements)} engagements")
                
                # Fetch user auth data
                logger.info("Fetching user authentication data...")
                cursor.execute("""
                    SELECT userid, fullname, statusid, subserviceline, officesupervisorid
                    FROM tymeplususerauth
                """)
                users = {row['userid']: row for row in cursor.fetchall()}
                logger.info(f"[OK] Fetched {len(users)} users")
                
                # Fetch supervisor list
                logger.info("Fetching supervisor list...")
                cursor.execute("""
                    SELECT employeeid, name FROM insytssupervisorlist
                """)
                supervisors = {row['employeeid']: row['name'] for row in cursor.fetchall()}
                logger.info(f"[OK] Fetched {len(supervisors)} supervisors")
                
                # Fetch billings
                logger.info("Fetching billing data...")
                cursor.execute("""
                    SELECT * FROM insytsbillings
                """)
                billings = cursor.fetchall()
                logger.info(f"[OK] Fetched {len(billings)} billing records")
                
                # Fetch disbursements
                logger.info("Fetching disbursement data...")
                cursor.execute("""
                    SELECT engagementid, claimsdisbursement, timesheetdisbursement
                    FROM insytsemployeemandays
                """)
                disbursements = {}
                for row in cursor.fetchall():
                    eng_id = row['engagementid']
                    if eng_id not in disbursements:
                        disbursements[eng_id] = []
                    disbursements[eng_id].append(row)
                logger.info(f"[OK] Fetched disbursements for {len(disbursements)} engagements")
                
                # Fetch timesheets (all rows) and pre-aggregate approved hours per engagement
                logger.info("Fetching timesheet data...")
                cursor.execute("""
                    SELECT * FROM insytstimesheets
                """)
                timesheets_raw = cursor.fetchall()
                logger.info(f"[OK] Fetched {len(timesheets_raw)} timesheet records")

                engagement_hours = self.build_timesheet_hours(timesheets_raw)
                logger.info(f"[OK] Aggregated approved hours for {len(engagement_hours)} engagements from timesheets")

                # Preload resource plan summaries across all resource plan tables
                resource_plan_tables = [
                    'insytsburundiresourceplan',
                    'insytsethiopiaresourceplan',
                    'insytsheadofficeresourceplan',
                    'insytsrwandaresourceplan',
                    'insytstanzaniaresourceplan',
                    'insytsugandaresourceplan',
                    'insytsresourceplan'
                ]
                resource_summaries = self.preload_resource_plan_summaries(cursor, resource_plan_tables)
            
            return engagements, users, supervisors, billings, disbursements, engagement_hours, resource_summaries
            
        except Exception as e:
            logger.error(f"[ERROR] Error fetching source data: {e}")
            raise
    
    def process_engagement(self, engagement, users, supervisors, billings, disbursements, engagement_hours, resource_summaries):
        """Process a single engagement record"""
        try:
            eng_id = engagement.get('engagementid')
            created_by = engagement.get('createdby')
            
            # Get user info
            user_info = users.get(created_by, {})
            fullname = user_info.get('fullname')
            status = user_info.get('statusid')
            subserviceline = self.get_subserviceline(user_info.get('subserviceline'))
            
            # Get manager
            supervisor_id = user_info.get('officesupervisorid')
            manager = supervisors.get(supervisor_id)
            
            # Parse engagement fields
            information = engagement.get('information')
            engagementname = self.parse_json_field(information, 'jobTitle')
            
            stakeholder = engagement.get('stakeholder')
            partner_email = self.parse_json_field(stakeholder, 'partner')
            pic_email = self.parse_json_field(stakeholder, 'personIncharge')
            
            partner = users.get(partner_email, {}).get('fullname')
            personincharge = users.get(pic_email, {}).get('fullname')
            
            client_data = engagement.get('client')
            managing_office_raw = self.parse_json_field(client_data, 'managingOffice')
            managingoffice = f"BDO EA {managing_office_raw}" if managing_office_raw else None
            serviceline = self.parse_json_field(client_data, 'serviceLine')
            clientid = self.parse_json_field(client_data, 'clientId')
            clientname = self.parse_json_field(client_data, 'clientName')
            
            jobcloser_data = engagement.get('jobcloser')
            jobcloser = self.parse_json_field(jobcloser_data, 'status')
            
            financial_data = engagement.get('financial_resourceplan')
            conversionrate = self.parse_json_field(financial_data, 'conversion_rate')
            usdamount = self.parse_json_field(financial_data, 'usdAmount')
            
            createdate = self.format_date(engagement.get('createdate'))
            closedate = self.format_date(engagement.get('approvedate'))
            
            # Process billings
            eng_billings = [b for b in billings if b.get('engagementid') == eng_id]
            billingid = eng_billings[0].get('billingid') if eng_billings else None
            bills = eng_billings[0].get('amount') if eng_billings else None
            billscreateddate = self.format_date(eng_billings[0].get('createdat')) if eng_billings else None
            
            # Billing closed/declined dates
            billscloseddate = None
            declineddate = None
            if eng_billings:
                billing_status = eng_billings[0].get('status')
                if billing_status == 'Approved by Partner':
                    billscloseddate = self.format_date(eng_billings[0].get('billingapproveddate'))
                elif billing_status == 'Approved by Finance':
                    billscloseddate = self.format_date(eng_billings[0].get('billingapproveddatefinance'))
                elif billing_status == 'Declined by Partner':
                    declineddate = self.format_date(eng_billings[0].get('billingdeclineddate'))
                elif billing_status == 'Declined by Finance':
                    declineddate = self.format_date(eng_billings[0].get('billingdeclineddatefinance'))
            
            # Get resource plan table
            resource_table = self.get_resource_plan_table(managingoffice)
            
            # Calculate chargeout and actualchargeout using preloaded aggregates
            chargeout = None
            actualchargeout = None

            table_cache = resource_summaries.get(resource_table, {})
            ext_rate = table_cache.get(eng_id, {}).get('rate', 0.0)
            mandays_val = table_cache.get(eng_id, {}).get('mandays', 0.0)

            if jobcloser == 'Not Started':
                chargeout = ext_rate
            elif jobcloser in ['Active', 'Not Approved', 'Closed']:
                total_hours = engagement_hours.get(eng_id, 0.0)
                actualchargeout = ext_rate * total_hours if ext_rate and total_hours else None
            
            # Calculate disbursement
            disbursement = 0.0
            if eng_id in disbursements:
                for disb in disbursements[eng_id]:
                    claims = disb.get('claimsdisbursement') or 0
                    timesheet_disb = disb.get('timesheetdisbursement') or 0
                    
                    if claims and float(claims) > 0:
                        disbursement += float(claims)
                    elif timesheet_disb and float(timesheet_disb) > 0:
                        disbursement += float(timesheet_disb)
            
            return {
                'engagementid': eng_id,
                'engagementname': engagementname,
                'createdate': createdate,
                'closedate': closedate,
                'partner': partner,
                'personincharge': personincharge,
                'managingoffice': managingoffice,
                'serviceline': serviceline,
                'clientid': clientid,
                'clientname': clientname,
                'fullname': fullname,
                'jobcloser': jobcloser,
                'billingid': billingid,
                'bills': bills,
                'billscreateddate': billscreateddate,
                'billscloseddate': billscloseddate,
                'declineddate': declineddate,
                'conversionrate': conversionrate,
                'usdamount': usdamount,
                'chargeout': chargeout,
                'actualchargeout': actualchargeout,
                'mandays': mandays_val,
                'disbursement': disbursement,
                'status': status,
                'subserviceline': subserviceline,
                'manager': manager
            }
            
        except Exception as e:
            logger.error(f"✗ Error processing engagement {engagement.get('engagementid')}: {e}")
            return None
    
    def upsert_data(self, records: List[Dict]):
        """Insert or update records in destination table"""
        logger.info(f"Upserting {len(records)} records...")
        
        upsert_sql = """
        INSERT INTO fullengagementlist (
            engagementid, engagementname, createdate, closedate, partner, personincharge,
            managingoffice, serviceline, clientid, clientname, fullname, jobcloser,
            billingid, bills, billscreateddate, billscloseddate, declineddate,
            conversionrate, usdamount, chargeout, actualchargeout, mandays,
            disbursement, status, subserviceline, manager, last_updated
        ) VALUES %s
        ON CONFLICT (engagementid) DO UPDATE SET
            engagementname = EXCLUDED.engagementname,
            createdate = EXCLUDED.createdate,
            closedate = EXCLUDED.closedate,
            partner = EXCLUDED.partner,
            personincharge = EXCLUDED.personincharge,
            managingoffice = EXCLUDED.managingoffice,
            serviceline = EXCLUDED.serviceline,
            clientid = EXCLUDED.clientid,
            clientname = EXCLUDED.clientname,
            fullname = EXCLUDED.fullname,
            jobcloser = EXCLUDED.jobcloser,
            billingid = EXCLUDED.billingid,
            bills = EXCLUDED.bills,
            billscreateddate = EXCLUDED.billscreateddate,
            billscloseddate = EXCLUDED.billscloseddate,
            declineddate = EXCLUDED.declineddate,
            conversionrate = EXCLUDED.conversionrate,
            usdamount = EXCLUDED.usdamount,
            chargeout = EXCLUDED.chargeout,
            actualchargeout = EXCLUDED.actualchargeout,
            mandays = EXCLUDED.mandays,
            disbursement = EXCLUDED.disbursement,
            status = EXCLUDED.status,
            subserviceline = EXCLUDED.subserviceline,
            manager = EXCLUDED.manager,
            last_updated = CURRENT_TIMESTAMP
        """
        
        try:
            with self.conn_dest.cursor() as cursor:
                values = [
                    (
                        r['engagementid'], r['engagementname'], r['createdate'], r['closedate'],
                        r['partner'], r['personincharge'], r['managingoffice'], r['serviceline'],
                        r['clientid'], r['clientname'], r['fullname'], r['jobcloser'],
                        r['billingid'], r['bills'], r['billscreateddate'], r['billscloseddate'],
                        r['declineddate'], r['conversionrate'], r['usdamount'], r['chargeout'],
                        r['actualchargeout'], r['mandays'], r['disbursement'], r['status'],
                        r['subserviceline'], r['manager'], datetime.now()
                    )
                    for r in records
                ]
                
                execute_values(cursor, upsert_sql, values)
                self.conn_dest.commit()
                logger.info(f"[OK] Successfully upserted {len(records)} records")
                
        except Exception as e:
            self.conn_dest.rollback()
            logger.error(f"[ERROR] Error upserting data: {e}")
            raise
    
    def run(self):
        """Execute the ETL pipeline"""
        start_time = datetime.now()
        logger.info("=" * 80)
        logger.info("ETL Pipeline Started")
        logger.info("=" * 80)
        
        try:
            # Connect to databases
            self.connect_databases()
            
            # Create destination table
            self.create_destination_table()
            
            # Fetch source data
            engagements, users, supervisors, billings, disbursements, engagement_hours, resource_summaries = self.fetch_source_data()
            
            # Process engagements
            logger.info("Processing engagement records...")
            processed_records = []
            failed_count = 0
            
            for i, engagement in enumerate(engagements, 1):
                if i % 100 == 0:
                    logger.info(f"Processing record {i}/{len(engagements)}...")
                
                record = self.process_engagement(
                    engagement, users, supervisors, billings, disbursements, engagement_hours, resource_summaries
                )
                
                if record:
                    processed_records.append(record)
                else:
                    failed_count += 1
            
            logger.info(f"✓ Processed {len(processed_records)} records successfully")
            if failed_count > 0:
                logger.warning(f"[WARNING] {failed_count} records failed processing")
            
            # Upsert data
            if processed_records:
                self.upsert_data(processed_records)
            
            # Summary
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            logger.info("=" * 80)
            logger.info("ETL Pipeline Completed Successfully")
            logger.info(f"Total Records Processed: {len(processed_records)}")
            logger.info(f"Failed Records: {failed_count}")
            logger.info(f"Duration: {duration:.2f} seconds")
            logger.info("=" * 80)
            
        except Exception as e:
            logger.error("=" * 80)
            logger.error("ETL Pipeline Failed")
            logger.error(f"Error: {str(e)}")
            logger.error("=" * 80)
            raise
        
        finally:
            self.close_connections()


def main():
    """Main execution function"""
    
    # Database configurations
    source_db_config = DatabaseConfig(
        host='bo-abc.com',
        port=5432,
        database='piie',
        user='ityn',
        password='qfg'
    )
    
    dest_db_config = DatabaseConfig(
        host='time.com',
        port=5432,
        database='Time',  # Timesheets DB
        user='time',
        password='fgd'
    )
    
    # Initialize and run ETL
    etl = EngagementETL(source_db_config, dest_db_config)
    etl.run()


if __name__ == "__main__":
    main()