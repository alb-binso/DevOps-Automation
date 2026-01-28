import boto3
from boto3.dynamodb.conditions import Key
import pandas as pd
from datetime import datetime
from typing import List, Dict, Optional
import logging
import re
import json
from decimal import Decimal
import psycopg2
from psycopg2.extras import execute_values
from psycopg2 import sql


# Custom JSON encoder for DynamoDB Decimal types
class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj) if obj % 1 else int(obj)
        return super(DecimalEncoder, self).default(obj)


# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ---------- Configuration ----------
CONFIG = {
    # DynamoDB Config
    "aws_access_key_id": "your_access_key_id",
    "aws_secret_access_key": "your_secret_access_key",
    "region_name": "your_region",
    "table_name": "TymeplusTasks",
    "tracking_client_id": "",

    # PostgreSQL Config
    "postgres_host": "your_postgres_host",
    "postgres_port": 5432,
    "postgres_database": "tymeplushr_prod",
    "postgres_user": "tymeplushr_readonly",
    "postgres_password": "your_postgres_password",
    "postgres_table": "trac_task",
    
    # Workflow JSON file path
    "workflow_json_path": "workflow.json"
}

# Type to Group mapping - NOW INCLUDING TYPE 4
TYPE_GROUP_MAP = {
    1: "Bug",
    2: "Issue",
    3: "New Task",
    4: "Request for Change"  # Previously skipped, now included
}

# Hardcoded project name mapping - exact matches
PROJECT_NAME_MAP = {
    # ---------------- FieldSale ----------------
    "field sale": "FieldSale",
    "field sale - (kenya)": "FieldSale",

    # ---------------- Insyts-BDO ----------------
    "insyts": "Insyts-BDO",
    "insyts - bdo": "Insyts-BDO",
    "insyts - bdo(client)": "Insyts-BDO",
    "insyts -bdo -(kenya)": "Insyts-BDO",
    "insyts -bdo -development": "Insyts-BDO",
    "insyts bdo": "Insyts-BDO",
    "insyts_bdo": "Insyts-BDO",  # Underscore variant

    # ---------------- Insyts-Global ----------------
    "insyts -bdo -global": "Insyts-Global",
    "insyts-global": "Insyts-Global",

    # ---------------- Insyts-Amadi ----------------
    "insyts amadi": "Insyts-Amadi",

    # ---------------- Nexus ----------------  
    "nexus": "Nexus",

    # ---------------- TrackRight ----------------
    "trackright": "TrackRight",
    "trackright - testing": "TrackRight",
    "trackright legacy system": "TrackRight",
    "trackright legacy system - (kenya)": "TrackRight",
    "trackright legacy system - development": "TrackRight",
    "trackright v2 - (kenya)": "TrackRight",
    "trackright v2 - development": "TrackRight",
    "trackright version 2": "TrackRight",
    "trackright version 2 - development": "TrackRight",

    # ---------------- TrackTask ----------------
    "tractask": "TrackTask",
    "tractask ": "TrackTask",
    "tractask(kenya)": "TrackTask",
    "tractask - development": "TrackTask",
    "tracktask": "TrackTask",
    "tracktask ": "TrackTask",
    "tracktask - development": "TrackTask",
    "tracktask - development ": "TrackTask",
    "tracktask(kenya)": "TrackTask",

    # ---------------- TymeplusHr → ForesytsHr ----------------
    "foresyts": "ForesytsHr",
    "foresyts - (kenya)": "ForesytsHr",
    "foresytshr": "ForesytsHr",
    "foresyts hr (westlands laser eye hospital ltd)": "ForesytsHr",
    "forsyts": "ForesytsHr",
    "tymeplushr": "ForesytsHr",
    "tymeplushr - aminika (kenya)": "ForesytsHr",
    "tymeplushr - dil": "ForesytsHr",
    "tymeplushr - dil - (kenya)": "ForesytsHr",
    "tymeplushr - demolive": "ForesytsHr",
    "tymeplushr - demolive - development": "ForesytsHr",
    "tymeplushr - demolive(kenya)": "ForesytsHr",
    "tymeplushr - phoenix": "ForesytsHr",
    "tymeplushr - phoenix (kenya)": "ForesytsHr",
    "tymeplushr - phoenix - development": "ForesytsHr",
    "tymeplushr - redginger": "ForesytsHr",
    "tymeplushr - redginger - (kenya)": "ForesytsHr",
    "tymeplushr - redginger - development": "ForesytsHr",
    "tymeplushr - wailasoft - (kenya)": "ForesytsHr",
    "tymeplushr - wailasoft - development": "ForesytsHr",
    "tymeplushr all server": "ForesytsHr",

    # ---------------- TymeplusPay → ForesytsPay ----------------
    "tymepluspay - demolive(kenya)": "ForesytsPay",
    "tymepluspay - phoenix (kenya)": "ForesytsPay",
    "tymepluspay - redginger": "ForesytsPay",
    "tymepluspay - redginger - (kenya)": "ForesytsPay",
    "tymepluspay": "ForesytsPay",
    "tymepluspay (mozambique)": "ForesytsPay",
    "tymepluspay (zambia)": "ForesytsPay",
    "tymepluspay - aminika (kenya)": "ForesytsPay",
    "tymepluspay - dil (kenya)": "ForesytsPay",
    "tymepluspay - demolive": "ForesytsPay",
    "tymepluspay - demolive - development": "ForesytsPay",
    "tymepluspay all servers": "ForesytsPay",
    "insyts - bdo pay": "ForesytsPay",
    "foresyts pay - all servers kenya": "ForesytsPay",
    "foresyts pay - westlands laser eye hospital": "ForesytsPay",

    # ---------------- QA ----------------
    "tymeplushr - demolive - testing": "QA",
    "tymeplushr - testing": "QA",
    "tymeplushr - wailasoft - testing": "QA",
    "tymeplushr-redginger-testing": "QA",
    "tymepluspay - demolive - testing": "QA",
    "tymepluspay - testing": "QA",
    "trackright - testing": "QA",
    "trackright legacy system - testing": "QA",
    "trackright v2 - testing": "QA",
    "insyts - testing": "QA",
    "insyts -bdo -testing": "QA",
    "qa": "QA",

    # ---------------- DevOps ----------------
    "devops - general": "DevOps",
    "devops - general (india)": "DevOps",
    "devops deployment project": "DevOps",
    "devops general project": "DevOps",

    # ---------------- PowerBI ----------------
    "powerbi": "PowerBI",

    # ---------------- Others ----------------
    "kenya team": "Others",
    "lms": "Others",
    "support system - development": "Others"
}


class WorkflowManager:
    """Manage workflow status mappings from JSON file"""
    
    def __init__(self, workflow_json_path: str):
        self.workflow_json_path = workflow_json_path
        self.workflow_status_maps = {}
        # Default fallback status map
        self.default_status_map = {
            "0": "Open",
            "1": "In Progress",
            "2": "Code Review",
            "3": "Ready for Testing",
            "4": "Re Open",
            "5": "Verified",
            "6": "Deployed",
            "7": "On hold",
            "8": "Cancelled",
            "9": "Closed",
            "10": "In Clarification",
            "11": "Ready for Deployment"
        }
        self.load_workflows()
    
    def load_workflows(self):
        """Load workflow configurations from JSON file"""
        try:
            with open(self.workflow_json_path, 'r') as f:
                workflow_data = json.load(f)
            
            workflow_list = workflow_data.get('workflowList', {})
            
            # Build status maps for each workflow
            for workflow_id, workflow_details_list in workflow_list.items():
                if workflow_details_list and len(workflow_details_list) > 0:
                    workflow_details = workflow_details_list[0]
                    workflow_flow = workflow_details.get('workFlow', [])
                    
                    # Create status map for this workflow
                    status_map = {}
                    for status_item in workflow_flow:
                        for status_id, status_name in status_item.items():
                            status_map[str(status_id)] = status_name.strip()
                    
                    self.workflow_status_maps[str(workflow_id)] = status_map
            
            logger.info(f"Loaded {len(self.workflow_status_maps)} workflow configurations")
            
        except FileNotFoundError:
            logger.error(f"Workflow JSON file not found: {self.workflow_json_path}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing workflow JSON: {e}")
            raise
    
    def get_status_name(self, workflow_id: str, status_id: str) -> str:
        """
        Get status name for a given workflow ID and status ID
        Falls back to default status map if workflow not found
        
        Args:
            workflow_id: The workflow ID
            status_id: The status ID (as string)
            
        Returns:
            Status name or default status name
        """
        workflow_id_str = str(workflow_id) if workflow_id else ""
        status_id_str = str(status_id)
        
        # Try workflow-specific status map first
        workflow_map = self.workflow_status_maps.get(workflow_id_str, {})
        if status_id_str in workflow_map:
            return workflow_map[status_id_str]
        
        # Fall back to default status map
        if status_id_str in self.default_status_map:
            return self.default_status_map[status_id_str]
        
        # Last resort: return unknown
        return f"Unknown Status ({status_id_str})"
    
    def get_workflow_info(self, workflow_id: str) -> Dict:
        """Get complete workflow information"""
        return self.workflow_status_maps.get(str(workflow_id), {})


class DynamoDBTaskExtractor:
    """Extract and process task data from DynamoDB with PostgreSQL sync"""

    def __init__(self, config: Dict, workflow_manager: WorkflowManager):
        self.config = config
        self.workflow_manager = workflow_manager
        self.dynamodb = self._connect_dynamodb()
        self.table = self.dynamodb.Table(config["table_name"])
        self.pg_conn = None
        self.pg_table = config["postgres_table"]

    def _connect_dynamodb(self):
        """Establish DynamoDB connection"""
        try:
            return boto3.resource(
                "dynamodb",
                aws_access_key_id=self.config["aws_access_key_id"],
                aws_secret_access_key=self.config["aws_secret_access_key"],
                region_name=self.config["region_name"],
            )
        except Exception as e:
            logger.error(f"Failed to connect to DynamoDB: {e}")
            raise

    def _connect_postgres(self):
        """Establish PostgreSQL connection"""
        try:
            self.pg_conn = psycopg2.connect(
                host=self.config["postgres_host"],
                port=self.config["postgres_port"],
                database=self.config["postgres_database"],
                user=self.config["postgres_user"],
                password=self.config["postgres_password"],
            )
            logger.info("PostgreSQL connection established")
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            raise

    def _create_table_if_not_exists(self):
        """Create PostgreSQL table if it doesn't exist"""
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {self.pg_table} (
            id SERIAL PRIMARY KEY,
            tracking_id VARCHAR(50) UNIQUE NOT NULL,
            workflowid VARCHAR(50),
            project_name VARCHAR(200),
            group_name VARCHAR(100),
            priority VARCHAR(50),
            status VARCHAR(100),
            status_id VARCHAR(10),
            employee VARCHAR(200),
            reported_date DATE,
            year INTEGER,
            month INTEGER,
            month_name VARCHAR(20),
            year_month VARCHAR(20),
            quarter VARCHAR(10),
            year_quarter VARCHAR(20),
            deadline DATE,
            deadtime TIME,
            closeddate DATE,
            closedtime TIME,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        try:
            cursor = self.pg_conn.cursor()
            cursor.execute(create_table_query)
            self.pg_conn.commit()
            cursor.close()
            logger.info(f"Table '{self.pg_table}' verified/created")
        except Exception as e:
            logger.error(f"Error creating table: {e}")
            raise

    def fetch_all_records(self, tracking_client_id: str) -> List[Dict]:
        """Fetch all records for a given tracking client ID with pagination"""
        items = []
        try:
            logger.info(f"Querying DynamoDB for trackingclientid: {tracking_client_id}")
            response = self.table.query(
                KeyConditionExpression=Key("trackingclientid").eq(tracking_client_id)
            )
            items.extend(response.get("Items", []))
            logger.info(f"Initial fetch: {len(items)} records")

            # Handle pagination
            while "LastEvaluatedKey" in response:
                response = self.table.query(
                    KeyConditionExpression=Key("trackingclientid").eq(tracking_client_id),
                    ExclusiveStartKey=response["LastEvaluatedKey"]
                )
                items.extend(response.get("Items", []))
                logger.info(f"Paginated fetch: Total {len(items)} records so far")

            logger.info(f"✅ Fetched {len(items)} total records from DynamoDB")
            return items

        except Exception as e:
            logger.error(f"❌ Error fetching records: {e}")
            raise

    def _standardize_project_name(self, raw_project_name: str) -> str:
        """Map raw project name to standardized project name"""
        if not raw_project_name:
            return "Others"

        normalized = raw_project_name.strip().lower()
        
        # Direct lookup first
        if normalized in PROJECT_NAME_MAP:
            return PROJECT_NAME_MAP[normalized]
        
        # Special handling for common variations
        # TymeplusPay (without space) → ForesytsPay
        if normalized == "tymepl uspay" or normalized == "tymepluspay":
            return "ForesytsPay"
        
        # TymeplusHr (without 'Hr' suffix in some records)
        if normalized == "tymeplus":
            return "ForesytsHr"
            
        return PROJECT_NAME_MAP.get(normalized, "Others")

    def _extract_employee_name(self, employee_str: str) -> str:
        """Extract employee name from string like 'Mr. John [ID:123]'"""
        if not employee_str:
            return ""
        
        # Remove title prefixes (Mr., Ms., Dr., etc.)
        employee_str = re.sub(r"^(Mr\.|Ms\.|Mrs\.|Dr\.|Prof\.)\s*", "", employee_str, flags=re.IGNORECASE)
        
        # Extract name before bracket if bracket exists
        match = re.match(r"^([^\[]+)", employee_str)
        if match:
            return match.group(1).strip()
        
        return employee_str.strip()

    def _parse_date_field(self, date_str: str) -> Optional[str]:
        """Parse date string to YYYY-MM-DD format with robust format detection"""
        if not date_str or date_str == "" or str(date_str).strip() == "":
            return None
        
        date_str = str(date_str).strip()
        
        # Try multiple date formats
        date_formats = [
            "%Y-%m-%dT%H:%M:%SZ",          # "2025-07-07T08:16:09Z" (ISO 8601 with Z) - MOST COMMON
            "%Y-%m-%dT%H:%M:%S.%fZ",       # "2025-08-19T13:11:00.000Z" (ISO with milliseconds and Z)
            "%Y-%m-%dT%H:%M:%S",           # "2025-08-19T13:11:00" (ISO timestamp)
            "%Y-%m-%dT%H:%M:%S.%f",        # "2025-08-19T13:11:00.000" (ISO with milliseconds)
            "%Y-%m-%d",                    # "2025-08-19" (ISO date only)
            "%d/%m/%Y %I:%M %p",           # "07/07/2025 01:41 PM"
            "%m/%d/%Y %I:%M %p",           # "07/07/2025 01:41 PM"
            "%B %d, %Y %I:%M %p",          # "July 07, 2025 01:41 PM"
            "%b %d, %Y %I:%M %p",          # "Jul 07, 2025 01:41 PM"
            "%d/%m/%Y",                    # "19/08/2025"
            "%m/%d/%Y",                    # "08/19/2025"
            "%Y/%m/%d",                    # "2025/08/19"
            "%d-%m-%Y",                    # "19-08-2025"
            "%Y-%m-%d %H:%M:%S",           # "2025-08-19 13:11:00"
            "%d %B %Y",                    # "19 August 2025"
            "%B %d %Y",                    # "August 19 2025"
        ]

        for fmt in date_formats:
            try:
                parsed_date = datetime.strptime(date_str, fmt)
                return parsed_date.strftime("%Y-%m-%d")
            except (ValueError, TypeError):
                continue
        
        # Try dateutil parser as last resort (handles many formats)
        try:
            from dateutil import parser
            parsed_date = parser.parse(date_str)
            return parsed_date.strftime("%Y-%m-%d")
        except:
            pass

        # If we get here, log the unparseable date for debugging
        if not hasattr(self, '_logged_date_samples'):
            self._logged_date_samples = 0
        
        if self._logged_date_samples < 5:  # Only log first 5 samples
            logger.warning(f"Could not parse date format: '{date_str}' (type: {type(date_str).__name__})")
            self._logged_date_samples += 1

        return None

    def _parse_time_field(self, time_str: str) -> Optional[str]:
        """Parse time string to HH:MM:SS format"""
        if not time_str or time_str == "":
            return None

        time_str = str(time_str).strip()

        # Handle ISO timestamp formats (extract time part)
        iso_formats = [
            "%Y-%m-%dT%H:%M:%SZ",          # "2025-07-07T08:16:09Z"
            "%Y-%m-%dT%H:%M:%S.%fZ",       # "2025-07-07T08:16:09.000Z"
            "%Y-%m-%dT%H:%M:%S",           # "2025-07-07T08:16:09"
            "%Y-%m-%d %H:%M:%S",           # "2025-07-07 08:16:09"
        ]
        
        for fmt in iso_formats:
            try:
                parsed_datetime = datetime.strptime(time_str, fmt)
                return parsed_datetime.strftime("%H:%M:%S")
            except ValueError:
                continue

        # Handle full datetime strings by extracting time part
        if "," in time_str or "/" in time_str:
            # Try to extract time from datetime strings like "July 30, 2025 02:47 PM"
            time_formats_with_date = [
                "%B %d, %Y %I:%M %p",       # "July 30, 2025 02:47 PM"
                "%B %d,%Y %I:%M %p",        # "July 30,2025 02:47 PM"
                "%d/%m/%Y %I:%M %p",        # "30/07/2025 02:47 PM"
                "%m/%d/%Y %I:%M %p",        # "07/30/2025 02:47 PM"
            ]
            for fmt in time_formats_with_date:
                try:
                    parsed_datetime = datetime.strptime(time_str, fmt)
                    return parsed_datetime.strftime("%H:%M:%S")
                except ValueError:
                    continue

        # Handle time-only strings
        time_formats = [
            "%I:%M %p",      # "02:47 PM"
            "%H:%M:%S",      # "14:47:00"
            "%H:%M",         # "14:47"
        ]

        for fmt in time_formats:
            try:
                parsed_time = datetime.strptime(time_str, fmt)
                return parsed_time.strftime("%H:%M:%S")
            except ValueError:
                continue

        # Try dateutil as last resort
        try:
            from dateutil import parser
            parsed_datetime = parser.parse(time_str)
            return parsed_datetime.strftime("%H:%M:%S")
        except:
            pass

        return None

    def transform_records(self, items: List[Dict]) -> pd.DataFrame:
        """Transform DynamoDB records into a pandas DataFrame with workflow-based status mapping"""
        transformed_rows = []
        skipped_count = 0
        skipped_reasons = {"missing_date": 0, "transform_error": 0}

        logger.info(f"Starting transformation of {len(items)} records...")
        
        # Debug: Show field names from first record
        if items:
            first_record = items[0]
            logger.info(f"Sample record keys: {list(first_record.keys())[:20]}...")  # Show first 20 keys
            logger.info(f"Sample reporteddate: {first_record.get('reporteddate', 'NOT FOUND')}")
            logger.info(f"Sample type: {first_record.get('type', 'NOT FOUND')}")
            logger.info(f"Sample priority: {first_record.get('priority', 'NOT FOUND')}")
        
        for idx, item in enumerate(items):
            try:
                # Extract basic fields - using ACTUAL DynamoDB field names
                tracking_client_id = item.get("trackingclientid", "")
                tracking_id_num = item.get("trackingid", "")
                
                # Use only the tracking number (e.g., "2077" not "WAX671O-2077")
                tracking_id = str(tracking_id_num) if tracking_id_num else ""
                
                workflow_id = str(item.get("workflowid", "")) if item.get("workflowid") else ""
                type_val = item.get("type", "")  # lowercase 'type'
                
                # Priority is a string like "Major", "Blocker - 1", "Blocker - 2"
                priority_raw = str(item.get("priority", "")).strip()
                
                # Status from trackingprojectstatusid
                status_id = str(item.get("trackingprojectstatusid", ""))
                
                # Project name from projectname (lowercase)
                project_name_raw = item.get("projectname", "")
                
                # Reported date from reporteddate (ISO format: "2025-07-07T08:16:09Z")
                reported_date_raw = item.get("reporteddate", "")
                
                # Employee from assignedto (email)
                employee_raw = item.get("assignedto", "")
                
                # Deadline from deadline (ISO format: "2025-07-08T00:16:09Z")
                deadline_raw = item.get("deadline", "")
                
                # No explicit closed date in DynamoDB, we'll derive it from status and history
                closeddate_raw = ""
                closedtime_raw = ""

                # Map group name based on Type
                # Map Others (type not in 1,2,3,4) to "New Task" as per requirement
                if type_val in [1, 2, 3, 4]:
                    group_name = TYPE_GROUP_MAP.get(type_val, "New Task")
                else:
                    group_name = "New Task"  # Others → New Task

                # Standardize project name
                project_name = self._standardize_project_name(project_name_raw)

                # Get status name from workflow manager using workflow ID and status ID
                status_name = self.workflow_manager.get_status_name(workflow_id, status_id)

                # Standardize priority values
                priority_lower = priority_raw.lower()
                if "blocker" in priority_lower:
                    if "1" in priority_raw or "- 1" in priority_raw:
                        priority = "Blocker - 1"
                    elif "2" in priority_raw or "- 2" in priority_raw:
                        priority = "Blocker - 2"
                    else:
                        priority = "Blocker - 1"  # Default blocker to Blocker - 1
                elif priority_lower == "major":
                    priority = "Major"
                elif priority_lower == "medium":
                    priority = "Medium"
                elif priority_lower == "minor" or priority_lower == "low":
                    priority = "Minor"
                elif priority_lower == "critical":
                    priority = "Critical"
                else:
                    priority = priority_raw if priority_raw else "Medium"  # Default to Medium

                # Extract employee name from email
                employee = self._extract_employee_name(employee_raw)

                # Parse reported date (ISO format)
                reported_date = self._parse_date_field(reported_date_raw)
                if not reported_date:
                    if idx < 3:  # Log first 3 failures
                        logger.warning(f"Record {tracking_id}: Missing/invalid reported date - raw value: '{reported_date_raw}'")
                    skipped_count += 1
                    skipped_reasons["missing_date"] += 1
                    continue

                # Try to find closed date from history if status is closed/completed
                status_lower = status_name.lower().strip()
                if status_lower in ["closed", "completed", "verified", " closed", " completed"]:
                    # Look for the last status change in history
                    history = item.get("history", [])
                    if isinstance(history, list):
                        for hist_entry in reversed(history):  # Start from most recent
                            if isinstance(hist_entry, dict):
                                actions = hist_entry.get("actions", {})
                                if "status_changed" in actions:
                                    # Found a status change, use this datetime
                                    closeddate_raw = hist_entry.get("datetime", "")
                                    closedtime_raw = closeddate_raw
                                    break
                    
                    # If still no closed date, use reported date
                    if not closeddate_raw:
                        closeddate_raw = reported_date_raw

                # Parse deadline and closed date/time
                deadline = self._parse_date_field(deadline_raw)
                deadtime = self._parse_time_field(deadline_raw) if deadline_raw else None
                closeddate = self._parse_date_field(closeddate_raw)
                closedtime = self._parse_time_field(closedtime_raw) if closedtime_raw else None

                # Calculate date dimensions
                date_obj = datetime.strptime(reported_date, "%Y-%m-%d")
                year = date_obj.year
                month = date_obj.month
                month_name = date_obj.strftime("%B")
                year_month = f"{year}-{month:02d}"
                quarter = f"Q{(month - 1) // 3 + 1}"
                year_quarter = f"{year}-{quarter}"

                # Build row
                row = {
                    "tracking_id": tracking_id,
                    "workflowid": workflow_id,
                    "project_name": project_name,
                    "group_name": group_name,
                    "priority": priority,
                    "status": status_name,
                    "status_id": status_id,
                    "employee": employee,
                    "reported_date": reported_date,
                    "year": year,
                    "month": month,
                    "month_name": month_name,
                    "year_month": year_month,
                    "quarter": quarter,
                    "year_quarter": year_quarter,
                    "deadline": deadline,
                    "deadtime": deadtime,
                    "closeddate": closeddate,
                    "closedtime": closedtime,
                }
                transformed_rows.append(row)

                # Log progress every 100 records
                if (idx + 1) % 100 == 0:
                    logger.info(f"   Processed {idx + 1}/{len(items)} records...")

            except Exception as e:
                logger.warning(f"Error transforming record {item.get('trackingid', 'unknown')}: {e}")
                import traceback
                if idx < 3:  # Show detailed error for first 3
                    traceback.print_exc()
                skipped_count += 1
                skipped_reasons["transform_error"] += 1
                continue

        df = pd.DataFrame(transformed_rows)
        
        logger.info(f"✅ Transformation complete:")
        logger.info(f"   - Valid records: {len(df)}")
        logger.info(f"   - Skipped records: {skipped_count}")
        if skipped_count > 0:
            logger.info(f"   - Skip reasons: {skipped_reasons}")
        
        return df

    def load_existing_data(self) -> pd.DataFrame:
        """Load existing data from PostgreSQL"""
        query = f"SELECT * FROM {self.pg_table}"
        try:
            df = pd.read_sql(query, self.pg_conn)
            logger.info(f"Loaded {len(df)} existing records from PostgreSQL")
            return df
        except Exception as e:
            logger.warning(f"Could not load existing data (table may be empty): {e}")
            return pd.DataFrame()

    def sync_data(self, new_df: pd.DataFrame):
        """Sync new data to PostgreSQL with comprehensive insert/update logic"""
        cursor = self.pg_conn.cursor()
        existing_df = self.load_existing_data()

        new_count = 0
        updated_count = 0

        def _none_if_blank(val):
            """Convert empty strings to None for SQL NULL"""
            if pd.isna(val) or val == "" or val == "None":
                return None
            return val

        if existing_df.empty:
            # Insert all records
            logger.info("Empty table - inserting all records...")
            columns = [
                "tracking_id", "workflowid", "project_name", "group_name",
                "priority", "status", "status_id", "employee",
                "reported_date", "year", "month", "month_name",
                "year_month", "quarter", "year_quarter",
                "deadline", "deadtime", "closeddate", "closedtime"
            ]

            values = [
                (
                    row["tracking_id"], row["workflowid"], row["project_name"], row["group_name"],
                    row["priority"], row["status"], row["status_id"], row["employee"],
                    row["reported_date"], row["year"], row["month"], row["month_name"],
                    row["year_month"], row["quarter"], row["year_quarter"],
                    _none_if_blank(row.get("deadline")),
                    _none_if_blank(row.get("deadtime")),
                    _none_if_blank(row.get("closeddate")),
                    _none_if_blank(row.get("closedtime"))
                )
                for _, row in new_df.iterrows()
            ]

            insert_query = f"""
            INSERT INTO {self.pg_table}
            ({', '.join(columns)})
            VALUES %s
            """
            execute_values(cursor, insert_query, values)
            self.pg_conn.commit()
            new_count = len(new_df)
            logger.info(f"✅ Inserted {new_count} records")

        else:
            # Check for updates and new records
            logger.info(f"Comparing with {len(existing_df)} existing records...")
            existing_ids = set(existing_df['tracking_id'].astype(str))

            for _, row in new_df.iterrows():
                tracking_id = str(row['tracking_id'])

                if tracking_id in existing_ids:
                    # Get existing record
                    existing_record = existing_df[existing_df['tracking_id'] == tracking_id].iloc[0]
                    
                    # Compare all important fields
                    old_status_id = str(existing_record.get('status_id', ''))
                    new_status_id = str(row['status_id'])
                    old_status = str(existing_record.get('status', ''))
                    new_status = str(row['status'])
                    old_employee = str(existing_record.get('employee', ''))
                    new_employee = str(row['employee'])
                    old_workflowid = str(existing_record.get('workflowid', '') or '')
                    new_workflowid = str(row.get('workflowid', '') or '')
                    old_project_name = str(existing_record.get('project_name', ''))
                    new_project_name = str(row['project_name'])
                    old_group_name = str(existing_record.get('group_name', ''))
                    new_group_name = str(row['group_name'])
                    old_priority = str(existing_record.get('priority', ''))
                    new_priority = str(row['priority'])
                    old_deadline = str(existing_record.get('deadline') or '')
                    new_deadline = str(row.get('deadline') or '')
                    old_deadtime = str(existing_record.get('deadtime') or '')
                    new_deadtime = str(row.get('deadtime') or '')
                    old_closeddate = str(existing_record.get('closeddate') or '')
                    new_closeddate = str(row.get('closeddate') or '')
                    old_closedtime = str(existing_record.get('closedtime') or '')
                    new_closedtime = str(row.get('closedtime') or '')

                    # Check if any field has changed
                    if (
                        old_status_id != new_status_id
                        or old_status != new_status
                        or old_employee != new_employee
                        or old_workflowid != new_workflowid
                        or old_project_name != new_project_name
                        or old_group_name != new_group_name
                        or old_priority != new_priority
                        or old_deadline != new_deadline
                        or old_deadtime != new_deadtime
                        or old_closeddate != new_closeddate
                        or old_closedtime != new_closedtime
                    ):
                        # Update the record with all fields
                        update_query = f"""
                        UPDATE {self.pg_table}
                        SET status = %s,
                            status_id = %s,
                            priority = %s,
                            employee = %s,
                            workflowid = %s,
                            project_name = %s,
                            group_name = %s,
                            deadline = %s,
                            deadtime = %s,
                            closeddate = %s,
                            closedtime = %s,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE tracking_id = %s
                        """
                        cursor.execute(update_query, (
                            row['status'],
                            row['status_id'],
                            row['priority'],
                            row['employee'],
                            row['workflowid'],
                            row['project_name'],
                            row['group_name'],
                            _none_if_blank(row.get('deadline')),
                            _none_if_blank(row.get('deadtime')),
                            _none_if_blank(row.get('closeddate')),
                            _none_if_blank(row.get('closedtime')),
                            tracking_id
                        ))
                        updated_count += 1
                        
                        # Log what changed
                        changes = []
                        if old_status != new_status:
                            changes.append(f"Status: {old_status} → {new_status}")
                        if old_employee != new_employee:
                            changes.append(f"Employee: {old_employee} → {new_employee}")
                        if old_workflowid != new_workflowid:
                            changes.append(f"Workflow: {old_workflowid} → {new_workflowid}")
                        if old_project_name != new_project_name:
                            changes.append(f"Project: {old_project_name} → {new_project_name}")
                        if old_closeddate != new_closeddate and new_closeddate:
                            changes.append(f"Closed: {new_closeddate}")
                        
                        if changes:
                            logger.info(f"Updated {tracking_id}: {', '.join(changes)}")
                else:
                    # Insert new record
                    insert_query = f"""
                    INSERT INTO {self.pg_table}
                    (tracking_id, workflowid, project_name, group_name, priority, status, status_id, employee,
                     reported_date, year, month, month_name, year_month, quarter, year_quarter,
                     deadline, deadtime, closeddate, closedtime)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    cursor.execute(insert_query, (
                        row['tracking_id'], row['workflowid'], row['project_name'], row['group_name'],
                        row['priority'], row['status'], row['status_id'], row['employee'],
                        row['reported_date'], row['year'], row['month'],
                        row['month_name'], row['year_month'], row['quarter'], row['year_quarter'],
                        _none_if_blank(row.get('deadline')),
                        _none_if_blank(row.get('deadtime')),
                        _none_if_blank(row.get('closeddate')),
                        _none_if_blank(row.get('closedtime'))
                    ))
                    new_count += 1
                    logger.info(f"Inserted new record: {tracking_id}")

            self.pg_conn.commit()
            logger.info(f"✅ Sync complete - New: {new_count}, Updated: {updated_count}")

        cursor.close()

        # Display statistics
        final_df = self.load_existing_data()

        print("\n" + "-" * 70)
        print("RECORDS BY PROJECT:")
        print("-" * 70)
        project_summary = final_df.groupby("project_name").size().sort_values(ascending=False)
        for project, count in project_summary.items():
            print(f"  {project:<20} : {count:>5} records")

        print("\n" + "-" * 70)
        print("RECORDS BY GROUP:")
        print("-" * 70)
        group_summary = final_df.groupby("group_name").size().sort_values(ascending=False)
        for group, count in group_summary.items():
            print(f"  {group:<25} : {count:>5} records")

        print("\n" + "-" * 70)
        print("RECORDS BY STATUS:")
        print("-" * 70)
        status_summary = final_df.groupby("status").size().sort_values(ascending=False)
        for status, count in status_summary.items():
            print(f"  {status:<25} : {count:>5} records")

        print("\n" + "-" * 70)
        print("RECORDS BY PRIORITY:")
        print("-" * 70)
        priority_summary = final_df.groupby("priority").size().sort_values(ascending=False)
        for priority, count in priority_summary.items():
            print(f"  {priority:<25} : {count:>5} records")

        print("\n" + "-" * 70)
        print("TOP 10 EMPLOYEES:")
        print("-" * 70)
        employee_summary = final_df[final_df["employee"] != ""].groupby("employee").size().sort_values(ascending=False).head(10)
        for employee, count in employee_summary.items():
            print(f"  {employee:<40} : {count:>5} records")

        print("\n" + "=" * 70)
        print(f"✅ SUCCESS!")
        print(f"   Database: {self.config['postgres_database']}")
        print(f"   Table: {self.pg_table}")
        print(f"   New Records Added: {new_count}")
        print(f"   Records Updated: {updated_count}")
        print(f"   Total Records: {len(final_df)}")
        print("=" * 70 + "\n")

    def close_connections(self):
        """Close database connections"""
        if self.pg_conn:
            self.pg_conn.close()
            logger.info("PostgreSQL connection closed")


def main():
    """Main execution function with incremental sync and status updates"""
    extractor = None
    try:
        print("\n" + "=" * 70)
        print("🚀 STARTING ETL PROCESS")
        print("=" * 70)
        
        # Step 1: Initialize workflow manager
        print("\n📋 Step 1: Loading workflow configurations...")
        workflow_manager = WorkflowManager(CONFIG["workflow_json_path"])
        print(f"✅ Loaded {len(workflow_manager.workflow_status_maps)} workflows")
        
        # Step 2: Initialize extractor with workflow manager
        print("\n📋 Step 2: Initializing database connections...")
        extractor = DynamoDBTaskExtractor(CONFIG, workflow_manager)

        # Step 3: Connect to PostgreSQL and create/alter table
        print("\n📋 Step 3: Setting up PostgreSQL...")
        extractor._connect_postgres()
        extractor._create_table_if_not_exists()
        print(f"✅ PostgreSQL ready - Table: {CONFIG['postgres_table']}")

        # Step 4: Fetch all records from DynamoDB
        print("\n📋 Step 4: Fetching records from DynamoDB...")
        print(f"   Client ID: {CONFIG['tracking_client_id']}")
        items = extractor.fetch_all_records(CONFIG["tracking_client_id"])

        if not items:
            logger.warning("⚠️ No records found in DynamoDB")
            print("\n❌ No records found - Please check:")
            print(f"   - DynamoDB table: {CONFIG['table_name']}")
            print(f"   - Client ID: {CONFIG['tracking_client_id']}")
            print(f"   - AWS credentials are correct")
            return

        # Step 5: Transform to DataFrame
        print(f"\n📋 Step 5: Transforming {len(items)} records...")
        new_df = extractor.transform_records(items)

        if new_df.empty:
            logger.warning("⚠️ No matching records found after filtering")
            print("\n⚠️ All records filtered out - Check Type filters (currently: 1,2,3,4)")
            return

        print(f"✅ Transformation complete: {len(new_df)} valid records")

        # Step 6: Check for duplicate TrackingIDs in source data
        print("\n📋 Step 6: Checking for duplicates...")
        duplicate_count = new_df["tracking_id"].duplicated().sum()
        if duplicate_count > 0:
            logger.warning(f"⚠️ Found {duplicate_count} duplicate TrackingIDs in source data")
            print(f"⚠️ Removing {duplicate_count} duplicates...")
            new_df = new_df.drop_duplicates(subset=["tracking_id"], keep="first")
            logger.info(f"Removed duplicates, {len(new_df)} unique records remaining")
        else:
            print("✅ No duplicates found")

        # Step 7: Sync data to PostgreSQL
        print("\n📋 Step 7: Syncing data to PostgreSQL...")
        print(f"   Target table: {CONFIG['postgres_database']}.{CONFIG['postgres_table']}")
        extractor.sync_data(new_df)

    except Exception as e:
        logger.error(f"❌ Execution failed: {e}")
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        if extractor:
            extractor.close_connections()


if __name__ == "__main__":
    main()