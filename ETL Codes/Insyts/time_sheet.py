import psycopg2
from psycopg2.extras import execute_values
import json
from datetime import datetime
import logging
from typing import List, Dict, Any, Optional
import sys

# Configure logging with UTF-8 encoding for Windows
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('timesheet_etl.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)

# Fix Windows console encoding
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')

logger = logging.getLogger(__name__)

class TimesheetETL:
    def __init__(self, source_config: Dict, dest_config: Dict):
        """
        Initialize ETL pipeline with source and destination database configurations.
        
        Args:
            source_config: Dict with keys: host, database, user, password, port
            dest_config: Dict with keys: host, database, user, password, port
        """
        self.source_config = source_config
        self.dest_config = dest_config
        self.source_conn = None
        self.dest_conn = None
        
    def connect_databases(self):
        """Establish connections to source and destination databases."""
        try:
            logger.info("Connecting to source database...")
            self.source_conn = psycopg2.connect(**self.source_config)
            logger.info("[OK] Source database connected")
            
            logger.info("Connecting to destination database...")
            self.dest_conn = psycopg2.connect(**self.dest_config)
            logger.info("[OK] Destination database connected")
        except Exception as e:
            logger.error(f"[ERROR] Database connection failed: {str(e)}")
            raise
    
    def close_connections(self):
        """Close database connections."""
        if self.source_conn:
            self.source_conn.close()
            logger.info("Source connection closed")
        if self.dest_conn:
            self.dest_conn.close()
            logger.info("Destination connection closed")
    
    def create_destination_table(self):
        """Create the destination table if it doesn't exist."""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS insytsalltimesheets (
            id SERIAL PRIMARY KEY,
            fullname VARCHAR(255),
            status INTEGER,
            country VARCHAR(255),
            serviceline VARCHAR(255),
            subserviceline VARCHAR(255),
            manager VARCHAR(255),
            department VARCHAR(255),
            engagementid VARCHAR(255),
            date DATE,
            timesheet_status VARCHAR(50),
            lastdate DATE,
            billshours NUMERIC(10, 2),
            amount NUMERIC(15, 2),
            userid VARCHAR(255),
            week VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(userid, engagementid, week)
        );
        
        CREATE INDEX IF NOT EXISTS idx_userid ON insytsalltimesheets(userid);
        CREATE INDEX IF NOT EXISTS idx_engagementid ON insytsalltimesheets(engagementid);
        CREATE INDEX IF NOT EXISTS idx_week ON insytsalltimesheets(week);
        """
        
        try:
            cursor = self.dest_conn.cursor()
            cursor.execute(create_table_sql)
            self.dest_conn.commit()
            logger.info("[OK] Destination table created/verified")
            cursor.close()
        except Exception as e:
            logger.error(f"[ERROR] Failed to create destination table: {str(e)}")
            raise
    
    def parse_json_array(self, value: Any, default: str = "") -> str:
        """Parse JSON array and return formatted string."""
        if not value:
            return default
        
        try:
            if isinstance(value, str):
                parsed = json.loads(value)
            else:
                parsed = value
            
            if isinstance(parsed, list) and len(parsed) > 0:
                if len(parsed) == 1:
                    return str(parsed[0])
                else:
                    return "Multi-Values"
            return default
        except:
            return default
    
    def format_country(self, managing_office: Any) -> str:
        """Format country with BDO EA prefix."""
        country = self.parse_json_array(managing_office)
        if country and country != "Multi-Values":
            return f"BDO EA {country}"
        return country if country else ""
    
    def format_serviceline(self, serviceline: Any) -> str:
        """Format serviceline field."""
        parsed = self.parse_json_array(serviceline)
        return "Multi-Services" if parsed == "Multi-Values" else parsed
    
    def format_subserviceline(self, subserviceline: Any) -> str:
        """Format subserviceline field."""
        parsed = self.parse_json_array(subserviceline)
        return "Multi-SubServices" if parsed == "Multi-Values" else parsed
    
    def format_department(self, department: Any) -> str:
        """Format department field."""
        parsed = self.parse_json_array(department)
        return "Multi-Departments" if parsed == "Multi-Values" else parsed
    
    def parse_week_to_date(self, week: str) -> Optional[datetime]:
        """Parse week string to end date (e.g., '2021AUG23-2021AUG29' -> 29-Aug-2021)."""
        if not week:
            return None
        
        try:
            # Extract end date from week range
            if '-' in week:
                end_part = week.split('-')[-1]
                # Parse format like '2021AUG29'
                date_obj = datetime.strptime(end_part, '%Y%b%d')
                return date_obj
        except Exception as e:
            logger.warning(f"Failed to parse week date '{week}': {str(e)}")
        
        return None
    
    def parse_updated_date(self, updated_at: str) -> Optional[datetime]:
        """Parse updated_at timestamp."""
        if not updated_at:
            return None
        
        try:
            # Parse ISO format timestamp
            date_obj = datetime.strptime(updated_at.split('T')[0], '%Y-%m-%d')
            return date_obj
        except Exception as e:
            logger.warning(f"Failed to parse updated date '{updated_at}': {str(e)}")
        
        return None
    
    def get_manager_name(self, supervisor_id: str, supervisor_dict: Dict) -> str:
        """Get manager name from supervisor dictionary."""
        if not supervisor_id:
            return ""
        
        return supervisor_dict.get(supervisor_id, "")
    
    def fetch_supervisors(self) -> Dict[str, str]:
        """Fetch supervisor mapping from insytssupervisorlist."""
        logger.info("Fetching supervisor list...")
        
        try:
            cursor = self.source_conn.cursor()
            cursor.execute("SELECT employeeid, name FROM insytssupervisorlist")
            supervisors = {row[0]: row[1] for row in cursor.fetchall()}
            cursor.close()
            logger.info(f"[OK] Loaded {len(supervisors)} supervisors")
            return supervisors
        except Exception as e:
            logger.error(f"[ERROR] Failed to fetch supervisors: {str(e)}")
            return {}
    
    def fetch_billings(self) -> Dict[str, float]:
        """Fetch billing amounts from insytsbillings."""
        logger.info("Fetching billing data...")
        
        try:
            cursor = self.source_conn.cursor()
            cursor.execute("SELECT engagementid, amount FROM insytsbillings")
            billings = {row[0]: float(row[1]) if row[1] else 0.0 for row in cursor.fetchall()}
            cursor.close()
            logger.info(f"[OK] Loaded {len(billings)} billing records")
            return billings
        except Exception as e:
            logger.error(f"[ERROR] Failed to fetch billings: {str(e)}")
            return {}
    
    def extract_timesheet_entries(self, timesheet_data: Any, week: str, updated_at: str) -> List[Dict]:
        """Extract individual timesheet entries from JSON array."""
        if not timesheet_data:
            return []
        
        try:
            if isinstance(timesheet_data, str):
                timesheets = json.loads(timesheet_data)
            else:
                timesheets = timesheet_data
            
            if not isinstance(timesheets, list):
                return []
            
            entries = []
            for ts in timesheets:
                engagement = ts.get('engagement', '')
                status = ts.get('status', '')
                sum_hours = ts.get('sum', '0')
                
                # Determine timesheet status
                ts_status = 'backlog' if status in ['draft', 'withdrawn'] else status
                
                # Parse date only if status is backlog
                date = None
                if ts_status == 'backlog':
                    date = self.parse_week_to_date(week)
                
                try:
                    hours = float(sum_hours) if sum_hours else 0.0
                except:
                    hours = 0.0
                
                entries.append({
                    'engagement': engagement,
                    'status': ts_status,
                    'hours': hours,
                    'date': date
                })
            
            return entries
        except Exception as e:
            logger.warning(f"Failed to parse timesheet data: {str(e)}")
            return []
    
    def deduplicate_data(self, data: List[tuple]) -> List[tuple]:
        """Remove duplicate entries based on unique constraint (userid, engagementid, week)."""
        seen = set()
        deduplicated = []
        duplicate_count = 0
        
        for row in data:
            # Extract userid (index 13), engagementid (index 7), week (index 14)
            userid = row[13]
            engagementid = row[7]
            week = row[14]
            
            key = (userid, engagementid, week)
            
            if key not in seen:
                seen.add(key)
                deduplicated.append(row)
            else:
                duplicate_count += 1
        
        if duplicate_count > 0:
            logger.warning(f"Removed {duplicate_count} duplicate entries")
        
        return deduplicated
    
    def fetch_and_transform_data(self) -> List[tuple]:
        """Fetch and transform data from source tables."""
        logger.info("Starting data extraction and transformation...")
        
        # Fetch reference data
        supervisors = self.fetch_supervisors()
        billings = self.fetch_billings()
        
        # Fetch main data
        query = """
        SELECT 
            u.userid,
            u.fullname,
            u.statusid,
            u.managingoffice,
            u.serviceline,
            u.subserviceline,
            u.officesupervisorid,
            u.leveldepartmentname,
            t.timesheet,
            t.week,
            t.updatedat
        FROM tymeplususerauth u
        LEFT JOIN insytstimesheets t ON u.userid = t.employeeid
        ORDER BY u.userid
        """
        
        try:
            cursor = self.source_conn.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()
            cursor.close()
            logger.info(f"[OK] Fetched {len(rows)} user records")
        except Exception as e:
            logger.error(f"[ERROR] Failed to fetch source data: {str(e)}")
            raise
        
        # Transform data
        transformed_data = []
        processed_users = 0
        users_with_timesheets = 0
        total_timesheet_entries = 0
        
        for row in rows:
            (userid, fullname, statusid, managingoffice, serviceline, 
             subserviceline, officesupervisorid, leveldepartmentname,
             timesheet, week, updatedat) = row
            
            processed_users += 1
            
            # Common fields for all timesheet entries
            common_data = {
                'userid': userid,
                'fullname': fullname or '',
                'status': int(statusid) if statusid is not None else 0,
                'country': self.format_country(managingoffice),
                'serviceline': self.format_serviceline(serviceline),
                'subserviceline': self.format_subserviceline(subserviceline),
                'manager': self.get_manager_name(officesupervisorid, supervisors),
                'department': self.format_department(leveldepartmentname),
                'lastdate': self.parse_updated_date(updatedat) if updatedat else None,
                'week': week or ''
            }
            
            # Extract timesheet entries
            if timesheet:
                users_with_timesheets += 1
                entries = self.extract_timesheet_entries(timesheet, week, updatedat)
                
                if entries:
                    for entry in entries:
                        total_timesheet_entries += 1
                        transformed_data.append((
                            common_data['fullname'],
                            common_data['status'],
                            common_data['country'],
                            common_data['serviceline'],
                            common_data['subserviceline'],
                            common_data['manager'],
                            common_data['department'],
                            entry['engagement'],
                            entry['date'],
                            entry['status'],
                            common_data['lastdate'],
                            entry['hours'],
                            billings.get(entry['engagement'], 0.0),
                            common_data['userid'],
                            common_data['week']
                        ))
        
        logger.info(f"[OK] Transformation complete:")
        logger.info(f"  - Total users processed: {processed_users}")
        logger.info(f"  - Users with timesheets: {users_with_timesheets}")
        logger.info(f"  - Total timesheet entries: {total_timesheet_entries}")
        
        # Deduplicate data before returning
        deduplicated_data = self.deduplicate_data(transformed_data)
        logger.info(f"[OK] Final record count after deduplication: {len(deduplicated_data)}")
        
        return deduplicated_data
    
    def upsert_data(self, data: List[tuple]):
        """Insert or update data in destination table."""
        if not data:
            logger.warning("No data to insert")
            return
        
        logger.info(f"Starting data upsert for {len(data)} records...")
        
        upsert_sql = """
        INSERT INTO insytsalltimesheets (
            fullname, status, country, serviceline, subserviceline, 
            manager, department, engagementid, date, timesheet_status, 
            lastdate, billshours, amount, userid, week, updated_at
        ) VALUES %s
        ON CONFLICT (userid, engagementid, week) 
        DO UPDATE SET
            fullname = EXCLUDED.fullname,
            status = EXCLUDED.status,
            country = EXCLUDED.country,
            serviceline = EXCLUDED.serviceline,
            subserviceline = EXCLUDED.subserviceline,
            manager = EXCLUDED.manager,
            department = EXCLUDED.department,
            date = EXCLUDED.date,
            timesheet_status = EXCLUDED.timesheet_status,
            lastdate = EXCLUDED.lastdate,
            billshours = EXCLUDED.billshours,
            amount = EXCLUDED.amount,
            updated_at = CURRENT_TIMESTAMP
        """
        
        try:
            cursor = self.dest_conn.cursor()
            
            # Add current timestamp to each record
            data_with_timestamp = [row + (datetime.now(),) for row in data]
            
            # Batch insert in chunks to handle large datasets
            batch_size = 5000
            total_batches = (len(data_with_timestamp) + batch_size - 1) // batch_size
            
            for i in range(0, len(data_with_timestamp), batch_size):
                batch = data_with_timestamp[i:i + batch_size]
                batch_num = (i // batch_size) + 1
                
                execute_values(
                    cursor,
                    upsert_sql,
                    batch,
                    template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                )
                
                logger.info(f"  - Processed batch {batch_num}/{total_batches} ({len(batch)} records)")
            
            self.dest_conn.commit()
            cursor.close()
            
            logger.info(f"[OK] Successfully upserted {len(data)} records")
        except Exception as e:
            self.dest_conn.rollback()
            logger.error(f"[ERROR] Failed to upsert data: {str(e)}")
            raise
    
    def run(self):
        """Execute the complete ETL pipeline."""
        logger.info("=" * 70)
        logger.info("TIMESHEET ETL PIPELINE STARTED")
        logger.info("=" * 70)
        
        try:
            # Connect to databases
            self.connect_databases()
            
            # Create destination table
            self.create_destination_table()
            
            # Extract and transform data
            transformed_data = self.fetch_and_transform_data()
            
            # Load data
            self.upsert_data(transformed_data)
            
            logger.info("=" * 70)
            logger.info("[SUCCESS] ETL PIPELINE COMPLETED SUCCESSFULLY")
            logger.info("=" * 70)
            
        except Exception as e:
            logger.error("=" * 70)
            logger.error(f"[FAILED] ETL PIPELINE FAILED: {str(e)}")
            logger.error("=" * 70)
            raise
        finally:
            self.close_connections()

if __name__ == "__main__":
    # Source database configuration
    SOURCE_CONFIG = {
        'host': 'b-hosting-server',
        'database': 'db-name',
        'user': 'db-username',
        'password': 'db-password',
        'port': 5432
    }
    
    # Destination database configuration
    DEST_CONFIG = {
        'host': 'b-hosting-server',
        'database': 'db-name',
        'user': 'db-username',
        'password': 'db-password',
        'port': 5432
    }
    
    # Run ETL pipeline
    etl = TimesheetETL(SOURCE_CONFIG, DEST_CONFIG)
    etl.run()