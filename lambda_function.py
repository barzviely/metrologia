import os
import json
import boto3
import uuid
import time
import shutil
from datetime import datetime, timezone, timedelta
from google.cloud import storage
import zipfile
from typing import Dict, Any, List
from tenacity import retry, stop_after_attempt, wait_exponential

def get_current_path():
    """Get current UTC time folder path: yyyy/MM/DD/HH"""
    now = datetime.now(timezone.utc)
    return now.strftime("%Y/%m/%d/%H")

def get_hour_path(hours_ago):
    """Get UTC time folder path for X hours ago: yyyy/MM/DD/HH"""
    now = datetime.now(timezone.utc) - timedelta(hours=hours_ago)
    return now.strftime("%Y/%m/%d/%H")

def get_storage_client():
    """Create a Google Cloud Storage client using credentials from AWS Secrets Manager"""
    try:
        # Get credentials from AWS Secrets Manager
        secret_client = boto3.client('secretsmanager')
        secret_response = secret_client.get_secret_value(
            SecretId=os.environ['GOOGLE_CREDS_SECRET_NAME']
        )
        credentials_dict = json.loads(secret_response['SecretString'])
        
        # Create storage client directly from service account info
        return storage.Client.from_service_account_info(credentials_dict)
    except Exception as e:
        print(f"Error setting up Google Storage client: {str(e)}")
        raise

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    reraise=True
)
def download_from_gcs_with_retry(blob):
    """Download from Google Cloud Storage with retry mechanism"""
    try:
        return blob.download_as_bytes()
    except Exception as e:
        print(f"Download attempt failed for {blob.name}: {str(e)}")
        raise

def put_metrics(cloudwatch, namespace: str, metrics_data: List[Dict]):
    """Helper function to send metrics to CloudWatch"""
    try:
        cloudwatch.put_metric_data(Namespace=namespace, MetricData=metrics_data)
    except Exception as e:
        print(f"Error sending metrics: {str(e)}")

def track_file_transfer(cloudwatch, file_name: str, file_size: int, duration: float, success: bool):
    """Track metrics for file transfer"""
    metrics = [
        {
            'MetricName': 'TransferDuration',
            'Value': duration,
            'Unit': 'Seconds',
            'Dimensions': [{'Name': 'FileName', 'Value': file_name}]
        },
        {
            'MetricName': 'FileSize',
            'Value': file_size,
            'Unit': 'Bytes',
            'Dimensions': [{'Name': 'FileName', 'Value': file_name}]
        },
        {
            'MetricName': 'TransferSuccess',
            'Value': 1 if success else 0,
            'Unit': 'Count',
            'Dimensions': [{'Name': 'FileName', 'Value': file_name}]
        }
    ]

    put_metrics(cloudwatch, 'MOD/FileTransfer', metrics)

def track_batch_processing(cloudwatch, total_files: int, successful_files: int, total_size: int, duration: float):
    """Track metrics for batch processing"""
    metrics = [
        {
            'MetricName': 'ProcessedFiles',
            'Value': total_files,
            'Unit': 'Count'
        },
        {
            'MetricName': 'SuccessfulTransfers',
            'Value': successful_files,
            'Unit': 'Count'
        },
        {
            'MetricName': 'TotalSize',
            'Value': total_size,
            'Unit': 'Bytes'
        },
        {
            'MetricName': 'BatchDuration',
            'Value': duration,
            'Unit': 'Seconds'
        }
    ]

    if total_files > 0:
        metrics.append({
            'MetricName': 'SuccessRate',
            'Value': (successful_files / total_files) * 100,
            'Unit': 'Percent'
        })

    put_metrics(cloudwatch, 'MOD/BatchProcessing', metrics)

def transfer_single_file(blob, destination_path, cloudwatch):
    """Transfer a single file from GCS to destination"""
    start_time = time.time()
    temp_dir = os.path.join('/tmp', str(uuid.uuid4()))
    os.makedirs(temp_dir, exist_ok=True)
    success = False
    
    try:
        # Download file from Google Cloud Storage
        print(f"Downloading {blob.name} from GCS")
        file_content = download_from_gcs_with_retry(blob)
        
        # Create the destination directory if it doesn't exist
        os.makedirs(os.path.dirname(destination_path), exist_ok=True)
        
        # Transfer the file to the destination
        print(f"Transferring {blob.name} to {destination_path}")
        with open(destination_path, 'wb') as f:
            f.write(file_content)
        
        success = True
        print(f"Successfully transferred {blob.name}")
        return True

    except Exception as e:
        print(f"Error transferring file {blob.name}: {str(e)}")
        return False
        
    finally:
        # Track metrics
        transfer_duration = time.time() - start_time
        track_file_transfer(
            cloudwatch,
            blob.name,
            blob.size,
            transfer_duration,
            success
        )
        
        # Clean up temporary files
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)

def lambda_handler(event, context):
    """Main Lambda handler"""
    batch_start_time = time.time()
    destination_base_path = os.environ.get('DESTINATION_PATH', '/mnt/efs')
    
    try:
        # Initialize clients
        cloudwatch = boto3.client('cloudwatch')
        
        # Get Google Storage client
        print("Initializing connection to Google Cloud Storage")
        storage_client = get_storage_client()
        
        # Get folder paths for the last 4 hours to ensure we capture all recent files
        folder_paths = []
        for i in range(4):
            folder_paths.append(get_hour_path(i))
        
        print(f"Checking for files in the last 4 hours: {folder_paths}")
        
        # Get all files from the last 4 hours
        gcs_bucket = storage_client.bucket(os.environ['GCS_BUCKET_NAME'])
        
        # List blobs from each hour and combine
        all_blobs = []
        for folder_path in folder_paths:
            print(f"Listing files in folder: {folder_path}")
            hour_blobs = list(gcs_bucket.list_blobs(prefix=folder_path))
            hour_zip_blobs = [blob for blob in hour_blobs if blob.name.endswith('.zip')]
            print(f"Found {len(hour_zip_blobs)} zip files in {folder_path}")
            all_blobs.extend(hour_zip_blobs)
        
        print(f"Found {len(all_blobs)} files to process from the last 4 hours")
        
        if not all_blobs:
            print("No files found to process")
            
            # Record metric for no files found
            put_metrics(cloudwatch, 'MOD/FileTransfer', [{
                'MetricName': 'NoFilesFound',
                'Value': 1,
                'Unit': 'Count'
            }])
            
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'No files to process',
                    'checked_folders': folder_paths
                })
            }
        
        total_size = sum(blob.size for blob in all_blobs)
        print(f"Total size of all files: {total_size} bytes")
        
        # Process files one by one
        results = []
        for blob in all_blobs:
            # Create destination path that preserves folder structure
            destination_file_path = os.path.join(destination_base_path, blob.name)
            result = transfer_single_file(blob, destination_file_path, cloudwatch)
            results.append(result)
        
        successful_files = sum(1 for r in results if r)
        
        # Track batch metrics
        batch_duration = time.time() - batch_start_time
        track_batch_processing(
            cloudwatch,
            len(all_blobs),
            successful_files,
            total_size,
            batch_duration
        )
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Processing complete',
                'total_files': len(all_blobs),
                'transferred_files': successful_files,
                'failed_transfers': len(all_blobs) - successful_files,
                'checked_folders': folder_paths,
                'total_size': total_size,
                'duration_seconds': batch_duration
            })
        }
        
    except Exception as e:
        print(f"Lambda execution failed: {str(e)}")
        import traceback
        print(traceback.format_exc())
        
        # Track metrics even in case of failure
        if 'cloudwatch' in locals():
            put_metrics(cloudwatch, 'MOD/FileTransfer', [{
                'MetricName': 'LambdaFailure',
                'Value': 1,
                'Unit': 'Count'
            }])
            
            if 'all_blobs' in locals():
                track_batch_processing(
                    cloudwatch,
                    len(all_blobs),
                    0,  # No successful files in case of failure
                    0,  # Don't report size in case of failure
                    time.time() - batch_start_time
                )
            
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'message': 'Lambda execution failed'
            })
        }
