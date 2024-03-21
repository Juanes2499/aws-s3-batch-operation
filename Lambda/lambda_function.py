import boto3
import uuid

def lambda_handler(event, context):

    # Initialize the Boto3 client for S3
    s3_control_client = boto3.client('s3control', region_name='us-east-1')

    # Parameters for the S3 Batch Operations job
    account_id = '211125768545' # ID account where the job is created and executed
    manifest_bucket = 'jn-poc-s3batch-manifest'  # Name of the bucket where your manifest file is stored
    manifest_file_key = 'manifest.csv'  # Name of the manifest file
    report_bucket = 'jn-poc-s3batch-report' # Name of the bucket where your report files are stored
    destination_bucket = 'jn-poc-s3batch-destination'  # Name of the destination bucket

    # Create the S3 Batch Operations job
    response = s3_control_client.create_job(
        AccountId=account_id,
        ConfirmationRequired=False,
        Operation={
            'S3PutObjectCopy': {
                'TargetResource': f'arn:aws:s3:::{destination_bucket}',
                # Additional copy operation parameters can be specified here
            }
        },
        Manifest={
            'Spec': {
                'Format': 'S3BatchOperations_CSV_20180820',
                'Fields': ['Bucket', 'Key']
            },
            'Location': {
                'ObjectArn': f'arn:aws:s3:::{manifest_bucket}/{manifest_file_key}',
                'ETag': 'a6e3ca72af154321b2e6b4aa405a80b2'  # ETag of the manifest file
            }
        },
        Report={
            'Bucket': f'arn:aws:s3:::{report_bucket}',
            'Format': 'Report_CSV_20180820',
            'Enabled': True,
            'Prefix': 'reports/',
            'ReportScope': 'AllTasks'
        },
        Priority=123,
        RoleArn='arn:aws:iam::211125768545:role/JN-PoC-S3BatchOperation-Role',  # ARN of the IAM role for S3 Batch Operation
        ClientRequestToken=str(uuid.uuid4()),  # Unique token for this job, for this case is UUID
        Description='PoC to create an execute a S3 Batch Operation'  # Job description
    )

    return response