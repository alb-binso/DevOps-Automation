import os
import json
import psycopg2
import boto3
import sys
from botocore.exceptions import ClientError
from dotenv import load_dotenv

# Load environment variables
load_dotenv("katros.env", override=True)
load_dotenv("live_credentials.env", override=True)  # Load live credentials

# Load email mappings
with open('email_mappings.json') as f:
    email_mappings = json.load(f)

# PostgreSQL DB connection template from .env
PG_CONN_STR_TEMPLATE = os.getenv("PG_CONN_STR_TEMPLATE")

# PostgreSQL schema map
POSTGRES_TABLE_MAP = {
    "tymeplushr-AMINICA": {
        "tymeplusmonthlyattendancereport": ["userid", "reportingmanager"],
        "tymeplususerbreak": ["userid", "reportingmanager"],
        "tymeplususerpunchactions": ["userid", "reportingmanager"]
    },
    "tymeplushr-DEV": {
        "tymeplusmonthlyattendancereport": ["userid", "reportingmanager"],
        "tymeplususerbreak": ["userid", "reportingmanager"],
        "tymeplususerpunchactions": ["userid", "reportingmanager"],
        "tymeplusHRClientList": ["email_id"]
    },
    "tymeplushr-DIL": {
        "tymeplushrlmsactivitylog": ["action_by"]
    },
    "tymeplushr-LMS": {
        "tymeplushrlmspeople": ["email"]
    },
    "tymeplushr-phoenix": {
        "tymeplusmonthlyattendancereport": ["userid", "reportingmanager"],
        "tymeplususerbreak": ["userid", "reportingmanager"],
        "tymeplususerpunchactions": ["userid", "reportingmanager"]
    },
    "tymeplushr-redginger": {
        "tymeplusmonthlyattendancereport": ["userid", "reportingmanager"],
        "tymeplususerbreak": ["userid", "reportingmanager"],
        "tymeplususerpunchactions": ["userid", "reportingmanager"]
    },
    "tymeplushr-wailasoft": {
        "tymeplusmonthlyattendancereport": ["userid", "reportingmanager"],
        "tymeplususerbreak": ["userid", "reportingmanager"],
        "tymeplususerpunchactions": ["userid", "reportingmanager"]
    }
}

# DynamoDB configuration
DYNAMODB = boto3.resource('dynamodb', region_name=os.getenv("AWS_REGION"))

# Initialize DynamoDB with new credentials for userAuthentication
LIVE_DYNAMODB = boto3.resource('dynamodb',
    region_name=os.getenv("LIVE_AWS_REGION"),
    aws_access_key_id=os.getenv("LIVE_AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("LIVE_AWS_SECRET_ACCESS_KEY")
)

DYNAMO_TABLE_MAP = {
    "tymeplusAllocationLeaveMaster": ["userid"],
    "tymeplusAppraisalDetails": ["usersCompleted"],
    "tymeplusAppraisalResponse": ["userId"],
    "tymeplusBand": ["userId"],
    "tymeplusCandidates": ["emailId"],
    "tymeplusClients": ["userId"],
    "tymeplusContact": ["userId"],
    "tymeplusCustomerDetails": ["userId"],
    "tymeplusDepartmentMaster": ["user_id"],
    "tymeplusDocuments": ["userId"],
    "tymeplusFeedback": ["userId"],
    "tymeplusHigherOfficials": ["email"],
    "tymeplushrBookaDemo": ["userId"],
    "tymeplushrDisciplinary": ["createdByUserId", "userId"],
    "tymeplushrincidentlog": ["userId"],
    "tymeplusHrPolicyLists": ["createdUserId"],
    "tymeplusLocationMaster": ["user_id"],
    "tymeplusMonthlyAttendanceReport": ["userId"],
    "tymeplusNotificationInOut": ["userId"],
    "tymeplusOnboardedCandidate": ["emailId"],
    "tymeplusOvertimeActionList": ["userId"],
    "tymeplusRoleList": ["userId"],
    "tymeplusTravelExpense": ["approvedBy", "declinedBy", "userId"],
    "tymeplusUserAbsentList": ["userId"],
    "tymeplusUserAuth": ["reportingmanager", "userid"],
    "tymeplusUserBreak": ["userId"],
    "tymeplusUserLeaves": ["userid", "approved_rejected_by", "reporting_manager"],
    "tymeplusUserMessages": ["userId"],
    "tymeplusUserPreference": ["userId"],
    "tymeplusUserPunchActions": ["userId"],
    "tymeplusUsersDraft": ["userid"],
    "userAuthentication": ["userId"]
}

def update_postgres():
    for db, tables in POSTGRES_TABLE_MAP.items():
        proceed = input(f"\nReady to update PostgreSQL DB: **{db}**. Proceed? (yes/no): ").strip().lower()
        if proceed != "yes":
            print(f"Skipping {db}")
            continue

        print(f"\nConnecting to PostgreSQL DB: {db}")
        try:
            conn = psycopg2.connect(PG_CONN_STR_TEMPLATE.format(db=db))
            cursor = conn.cursor()
            for table, cols in tables.items():
                print(f"\nProcessing table: {table}")
                for col in cols:
                    updates_count = 0
                    skips_count = 0
                    deletes_count = 0
                    for mapping in email_mappings:
                        old, new = mapping['old'], mapping['new']
                        try:
                            # First try to update
                            cursor.execute(f'UPDATE "{table}" SET "{col}" = %s WHERE "{col}" = %s', (new, old))
                            if cursor.rowcount > 0:
                                print(f"Updated {table}.{col}: {old} -> {new}")
                                updates_count += 1
                            
                            # If update didn't affect any rows, check if old value exists
                            if cursor.rowcount == 0:
                                cursor.execute(f'SELECT 1 FROM "{table}" WHERE "{col}" = %s LIMIT 1', (old,))
                                if cursor.fetchone():
                                    # Old value exists but update failed, try to delete
                                    try:
                                        cursor.execute(f'DELETE FROM "{table}" WHERE "{col}" = %s', (old,))
                                        print(f"Deleted {table}.{col} row for {old} after failed update")
                                        deletes_count += 1
                                    except Exception as de:
                                        print(f"❌ Error deleting {table}.{col} for {old}: {de}")
                                else:
                                    print(f"No rows found with {table}.{col} = {old}")
                                    skips_count += 1
                        except Exception as e:
                            print(f"❌ Error updating {table}.{col}: {e}")
                            # Try to delete the old value if update failed
                            try:
                                cursor.execute(f'DELETE FROM "{table}" WHERE "{col}" = %s', (old,))
                                print(f"Deleted {table}.{col} row for {old} after error")
                                deletes_count += 1
                            except Exception as de:
                                print(f"❌ Error deleting {table}.{col} for {old}: {de}")
                    
                    print(f"Summary for {table}.{col}: {updates_count} updates, {skips_count} skips, {deletes_count} deletes")
            conn.commit()
            cursor.close()
            conn.close()
        except Exception as e:
            print(f"❌ Error in DB {db}: {e}")

def update_dynamodb():
    # Tables that contain array/list of email values
    array_email_tables = {
        "tymeplusAppraisalDetails": ["usersCompleted"],
        "tymeplusDepartmentMaster": ["user_id"],
        "tymeplusLocationMaster": ["user_id"],
        "tymeplusRoleList": ["userId"]
    }

    def normalize_email(email):
        if isinstance(email, dict) and 'S' in email:
            return email['S'].lower().strip()
        return str(email).lower().strip()

    def merge_items(old_item, existing_item, field):
        """Merge data from old item into existing item, preserving non-null values"""
        merged = existing_item.copy()
        for key, value in old_item.items():
            if key == field:
                continue
            if key not in existing_item or existing_item[key] is None:
                merged[key] = value
        return merged

    for table_name, fields in DYNAMO_TABLE_MAP.items():
        print(f"\nProcessing DynamoDB Table: {table_name}")
        table = DYNAMODB.Table(table_name)
        try:
            updates_count = 0
            skips_count = 0
            deletes_count = 0
            items_processed = 0
            
            # Handle pagination for scan
            last_evaluated_key = None
            while True:
                if last_evaluated_key:
                    response = table.scan(ExclusiveStartKey=last_evaluated_key)
                else:
                    response = table.scan()
                
                items = response.get('Items', [])
                items_processed += len(items)
                
                key_schema = [k['AttributeName'] for k in table.key_schema]
                
                for item in items:
                    for mapping in email_mappings:
                        old, new = mapping['old'], mapping['new']
                        old = old.lower().strip()
                        new = new.lower().strip()
                        
                        for field in fields:
                            if field not in item:
                                continue

                            # Handle array fields
                            if table_name in array_email_tables and field in array_email_tables[table_name]:
                                if isinstance(item[field], list):
                                    updated = False
                                    new_array = []
                                    for email_item in item[field]:
                                        if normalize_email(email_item) == old:
                                            if isinstance(email_item, dict) and 'S' in email_item:
                                                new_array.append({'S': new})
                                            else:
                                                new_array.append(new)
                                            updated = True
                                        else:
                                            new_array.append(email_item)
                                    
                                    if updated:
                                        try:
                                            key = {k: item[k] for k in key_schema}
                                            table.update_item(
                                                Key=key,
                                                UpdateExpression=f"SET {field} = :new",
                                                ExpressionAttributeValues={":new": new_array}
                                            )
                                            print(f"Updated array in {table_name}.{field}: {old} -> {new}")
                                            updates_count += 1
                                        except Exception as e:
                                            print(f"❌ Error updating array in {table_name}.{field}: {e}")
                                continue

                            # Handle single value fields
                            if normalize_email(item[field]) == old:
                                try:
                                    key = {k: item[k] for k in key_schema}
                                    # Prepare new item with all data from old item
                                    new_item = item.copy()
                                    new_item[field] = new

                                    # Check if new item exists
                                    new_key = {k: new_item[k] for k in key_schema}
                                    try:
                                        existing_item = table.get_item(Key=new_key)
                                        
                                        if 'Item' in existing_item:
                                            # Compare and merge data
                                            print(f"\nComparing items for {old} -> {new}:")
                                            print("Existing item fields:", list(existing_item['Item'].keys()))
                                            print("Old item fields:", list(item.keys()))
                                            
                                            # Merge data from old item into existing item
                                            merged_item = merge_items(item, existing_item['Item'], field)
                                            
                                            # Log what's being merged
                                            print("\nMerging data:")
                                            for key, value in merged_item.items():
                                                if key in existing_item['Item'] and existing_item['Item'][key] != value:
                                                    print(f"- Updating {key}: {existing_item['Item'][key]} -> {value}")
                                                elif key not in existing_item['Item']:
                                                    print(f"- Adding new field {key}: {value}")
                                            
                                            # Update existing item with merged data
                                            table.put_item(Item=merged_item)
                                            print(f"\nSuccessfully merged data for {table_name}.{field}: {old} -> {new}")
                                            
                                            # Delete old item - ensure we're using the correct key structure
                                            delete_key = {k: item[k] for k in key_schema}
                                            table.delete_item(Key=delete_key)
                                            print(f"Deleted old item in {table_name} with {field}: {old}")
                                            updates_count += 1
                                            deletes_count += 1
                                        else:
                                            # If no existing item, create new one
                                            table.put_item(Item=new_item)
                                            print(f"Created new item in {table_name} with {field}: {new}")
                                            
                                            # Verify the new item was created successfully
                                            verify_item = table.get_item(Key=new_key)
                                            if 'Item' in verify_item:
                                                # Delete old item after successful creation
                                                delete_key = {k: item[k] for k in key_schema}
                                                table.delete_item(Key=delete_key)
                                                print(f"Deleted old item in {table_name} with {field}: {old}")
                                                updates_count += 1
                                                deletes_count += 1
                                            else:
                                                print(f"❌ Error: Could not verify new item creation for {old} -> {new}")
                                    except Exception as e:
                                        print(f"❌ Error checking/merging items in {table_name}: {e}")
                                        continue
                                except Exception as outer_e:
                                    print(f"❌ Error processing item in {table_name}.{field}: {outer_e}")
                
                # Check if there are more items to scan
                last_evaluated_key = response.get('LastEvaluatedKey')
                if not last_evaluated_key:
                    break
            
            print(f"Summary for {table_name}: {updates_count} updates, {skips_count} skips, {deletes_count} deletes, {items_processed} items processed")
        except Exception as e:
            print(f"❌ Error scanning DynamoDB table {table_name}: {e}")

def update_live_user_authentication():
    """
    Update userAuthentication table in live environment with new AWS credentials
    using email mappings from the JSON file
    """
    # Ask for permission to proceed with live DynamoDB
    proceed = input("\n⚠️  WARNING: You are about to connect to LIVE DynamoDB. Proceed? (yes/no): ").strip().lower()
    if proceed != "yes":
        print("❌ Live DynamoDB update cancelled by user")
        return

    table = LIVE_DYNAMODB.Table('userAuthentication')
    
    try:
        # Get the key schema first
        key_schema = table.key_schema
        print(f"Table key schema: {key_schema}")
        
        # Extract partition key and sort key (if any)
        partition_key = next((k['AttributeName'] for k in key_schema if k['KeyType'] == 'HASH'), None)
        sort_key = next((k['AttributeName'] for k in key_schema if k['KeyType'] == 'RANGE'), None)
        
        if not partition_key:
            print("❌ Error: Could not determine partition key from schema")
            return
            
        print(f"Partition key: {partition_key}")
        if sort_key:
            print(f"Sort key: {sort_key}")
        
        updates_count = 0
        skips_count = 0
        deletes_count = 0
        items_processed = 0
        errors_count = 0
        
        # Handle pagination for scan
        last_evaluated_key = None
        while True:
            if last_evaluated_key:
                response = table.scan(ExclusiveStartKey=last_evaluated_key)
            else:
                response = table.scan()
            
            items = response.get('Items', [])
            items_processed += len(items)
            
            for item in items:
                for mapping in email_mappings:
                    old, new = mapping['old'], mapping['new']
                    client_id = mapping.get('clientId')  # clientId is optional
                    old = old.lower().strip()
                    new = new.lower().strip()
                    
                    # Check if required key fields exist
                    if partition_key not in item:
                        print(f"Skipped: Item missing partition key {partition_key}")
                        skips_count += 1
                        continue
                        
                    if sort_key and sort_key not in item:
                        print(f"Skipped: Item missing sort key {sort_key}")
                        skips_count += 1
                        continue

                    # Check if old email matches
                    if item[partition_key].lower().strip() == old:
                        # Handle clientId matching
                        item_client_id = item.get('clientId')
                        if client_id:
                            if not item_client_id:
                                print(f"Skipped: Item has no clientId but mapping requires {client_id}")
                                skips_count += 1
                                continue
                            if item_client_id != client_id:
                                continue
                        else:
                            if not item_client_id:
                                print(f"Warning: Item has no clientId, proceeding with email-only match")

                        try:
                            # Check if new email exists
                            check_key = {partition_key: new}
                            if sort_key:
                                check_key[sort_key] = item[sort_key]
                            
                            existing_item = None
                            try:
                                check_response = table.get_item(Key=check_key)
                                if 'Item' in check_response:
                                    existing_item = check_response['Item']
                                    print(f"Found existing item with email {new}" + 
                                          (f" for client {client_id}" if client_id else ""))
                            except Exception as e:
                                print(f"Error checking new email: {e}")
                                errors_count += 1
                                continue

                            # Create new item with updated email
                            new_item = item.copy()
                            new_item[partition_key] = new
                            
                            if existing_item:
                                # Merge data from old item into existing item
                                for key, value in item.items():
                                    if key != partition_key and (key not in existing_item or existing_item[key] is None):
                                        existing_item[key] = value
                                
                                # Update existing item with merged data
                                table.put_item(Item=existing_item)
                                print(f"Updated existing item with merged data for {new}" + 
                                      (f" for client {client_id}" if client_id else ""))
                            else:
                                # Insert new item
                                table.put_item(Item=new_item)
                                print(f"Created new item with email {new}" + 
                                      (f" for client {client_id}" if client_id else ""))
                            
                            # Verify item exists
                            verify_key = {partition_key: new}
                            if sort_key:
                                verify_key[sort_key] = item[sort_key]
                                
                            verify_response = table.get_item(Key=verify_key)
                            
                            if 'Item' in verify_response:
                                # Compare all fields except partition key
                                new_data = verify_response['Item'].copy()
                                old_data = item.copy()
                                
                                # Remove partition key from both
                                new_data.pop(partition_key, None)
                                old_data.pop(partition_key, None)
                                
                                # Get common fields
                                common_fields = set(new_data.keys()) & set(old_data.keys())
                                
                                # Compare only common fields
                                data_matches = all(new_data[field] == old_data[field] for field in common_fields)
                                
                                if data_matches:
                                    # Delete old item
                                    delete_key = {partition_key: old}
                                    if sort_key:
                                        delete_key[sort_key] = item[sort_key]
                                        
                                    table.delete_item(Key=delete_key)
                                    print(f"Deleted old item with email {old}" + 
                                          (f" for client {client_id}" if client_id else ""))
                                    updates_count += 1
                                    deletes_count += 1
                                else:
                                    print(f"Warning: Data mismatch between old and new items for {old} -> {new}" + 
                                          (f" in client {client_id}" if client_id else ""))
                                    print("Keeping both items for manual review")
                                    errors_count += 1
                            else:
                                print(f"Error: Could not verify item creation for {old} -> {new}" + 
                                      (f" in client {client_id}" if client_id else ""))
                                errors_count += 1
                                
                        except Exception as e:
                            print(f"Error processing item: {e}")
                            errors_count += 1
            
            # Check if there are more items to scan
            last_evaluated_key = response.get('LastEvaluatedKey')
            if not last_evaluated_key:
                break
        
        print(f"\nSummary for userAuthentication:")
        print(f"- Updates: {updates_count}")
        print(f"- Skips: {skips_count}")
        print(f"- Deletes: {deletes_count}")
        print(f"- Errors: {errors_count}")
        print(f"- Items processed: {items_processed}")
        
    except Exception as e:
        print(f"Error accessing DynamoDB: {e}")

if __name__ == "__main__":
    update_postgres()
    update_dynamodb()
    update_live_user_authentication()
    print("\n✅ Migration complete.")
