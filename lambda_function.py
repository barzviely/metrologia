import json
import boto3
import base64
from google.cloud import storage
from google.oauth2 import service_account
import tempfile
import os
import datetime

def lambda_handler(event, context):
    print("Lambda function started")
    
    try:
        # Get environment variables
        secret_name = os.environ.get('GOOGLE_CREDS_SECRET_NAME')
        gcs_bucket_name = os.environ.get('GCS_BUCKET_NAME', '').strip()
        
        print(f"Secret name: {secret_name}")
        print(f"Bucket name: '{gcs_bucket_name}'")
        
        if not gcs_bucket_name:
            return {
                'statusCode': 400,
                'body': json.dumps('GCS bucket name is required')
            }
        
        # Get GCS credentials from AWS Secrets Manager
        print("Retrieving secret from AWS Secrets Manager")
        secrets_client = boto3.client('secretsmanager')
        response = secrets_client.get_secret_value(SecretId=secret_name)
        
        if 'SecretString' in response:
            secret = response['SecretString']
        else:
            secret = base64.b64decode(response['SecretBinary'])
        
        print("Secret retrieved successfully")
        
        # Create temporary credentials file
        with tempfile.NamedTemporaryFile(mode='w+', delete=False) as temp:
            temp.write(secret)
            temp_filename = temp.name
        
        try:
            # Initialize GCS client with credentials
            print("Initializing GCS client")
            credentials = service_account.Credentials.from_service_account_file(temp_filename)
            storage_client = storage.Client(credentials=credentials, project=credentials.project_id)
            print(f"GCS client created for project: {credentials.project_id}")
            
            # Try to access the specific bucket directly
            print(f"Accessing bucket '{gcs_bucket_name}' directly")
            bucket = storage_client.bucket(gcs_bucket_name)
            
            # Check if bucket exists
            if not bucket.exists():
                print(f"Bucket '{gcs_bucket_name}' does not exist or you don't have access to it")
                return {
                    'statusCode': 404,
                    'body': json.dumps({
                        'success': False,
                        'message': f"Bucket '{gcs_bucket_name}' does not exist or you don't have access to it"
                    })
                }
            
            # Calculate timestamp for 2 hours ago
            two_hours_ago = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=2)
            print(f"Filtering for files created/updated after: {two_hours_ago.isoformat()}")
            
            # List all objects
            print(f"Listing objects in bucket '{gcs_bucket_name}'")
            blobs = list(bucket.list_blobs())
            
            # Filter for files created/updated in the last 2 hours
            recent_files = []
            for blob in blobs:
                if blob.updated and blob.updated > two_hours_ago:
                    recent_files.append({
                        'name': blob.name,
                        'updated': blob.updated.isoformat() if blob.updated else None,
                        'size': blob.size,
                        'content_type': blob.content_type
                    })
            
            print(f"Found {len(recent_files)} files from the last 2 hours")
            
            # Look for files in the folder structure specified in the requirements
            # Format: yyyy/MM/DD/HH
            now = datetime.datetime.now(datetime.timezone.utc)
            current_prefix = now.strftime("%Y/%m/%d/%H")
            previous_hour = now - datetime.timedelta(hours=1)
            previous_prefix = previous_hour.strftime("%Y/%m/%d/%H")
            
            print(f"Checking for files in current hour folder: {current_prefix}")
            print(f"Checking for files in previous hour folder: {previous_prefix}")
            
            current_hour_files = [f for f in recent_files if f['name'].startswith(current_prefix)]
            previous_hour_files = [f for f in recent_files if f['name'].startswith(previous_prefix)]
            
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'success': True,
                    'message': f'Successfully connected to GCS bucket: {gcs_bucket_name}',
                    'total_recent_files': len(recent_files),
                    'current_hour_folder': current_prefix,
                    'current_hour_files': current_hour_files,
                    'previous_hour_folder': previous_prefix,
                    'previous_hour_files': previous_hour_files
                }, default=str)
            }
        
        finally:
            # Clean up the temporary file
            os.unlink(temp_filename)
            print("Temporary file deleted")
    
    except Exception as e:
        print(f"Error in Lambda execution: {str(e)}")
        import traceback
        print(traceback.format_exc())
        return {
            'statusCode': 500,
            'body': json.dumps({
                'success': False,
                'message': f'Error: {str(e)}'
            })
        }
