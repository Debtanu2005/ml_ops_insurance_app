import os
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from datetime import datetime

print("=" * 60)
print("AWS CREDENTIALS DIAGNOSTIC REPORT")
print("=" * 60)
print(f"Timestamp: {datetime.now()}")
print()

# Check environment variables
print("1. CHECKING ENVIRONMENT VARIABLES:")
print("-" * 60)
access_key = os.getenv("AWS_ACCESS_KEY_ID")
secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
region = os.getenv("AWS_DEFAULT_REGION", "us-east-1")

if access_key:
    print(f"✓ AWS_ACCESS_KEY_ID found (length: {len(access_key)})")
else:
    print("✗ AWS_ACCESS_KEY_ID not found")

if secret_key:
    print(f"✓ AWS_SECRET_ACCESS_KEY found (length: {len(secret_key)})")
else:
    print("✗ AWS_SECRET_ACCESS_KEY not found")

print(f"Region: {region}")
print()

# Test basic S3 connection
print("2. TESTING S3 CONNECTION:")
print("-" * 60)
try:
    s3_client = boto3.client(
        's3',
        region_name=region,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )
    
    # Try to list buckets (requires S3 permissions)
    response = s3_client.list_buckets()
    buckets = [b['Name'] for b in response['Buckets']]
    print(f"✓ Successfully connected to S3")
    print(f"✓ Found {len(buckets)} bucket(s): {buckets}")
    print()
    
    # Test credentials validity
    print("3. TESTING CREDENTIALS VALIDITY:")
    print("-" * 60)
    sts_client = boto3.client(
        'sts',
        region_name=region,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )
    identity = sts_client.get_caller_identity()
    print(f"✓ Credentials are VALID")
    print(f"  Account: {identity['Account']}")
    print(f"  ARN: {identity['Arn']}")
    print()
    
    # Test multipart upload capability
    print("4. TESTING MULTIPART UPLOAD:")
    print("-" * 60)
    if buckets:
        test_bucket = buckets[0]
        test_key = "test-multipart-upload.txt"
        
        try:
            mpu = s3_client.create_multipart_upload(
                Bucket=test_bucket,
                Key=test_key
            )
            upload_id = mpu['UploadId']
            print(f"✓ Multipart upload initiated on bucket '{test_bucket}'")
            
            # Abort the multipart upload
            s3_client.abort_multipart_upload(
                Bucket=test_bucket,
                Key=test_key,
                UploadId=upload_id
            )
            print(f"✓ Multipart upload aborted successfully")
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            error_msg = e.response['Error']['Message']
            print(f"✗ Multipart upload failed: {error_code}")
            print(f"  Message: {error_msg}")
            if error_code == "SignatureDoesNotMatch":
                print("  → This suggests credential issues or clock skew")
            elif error_code == "AccessDenied":
                print("  → This suggests insufficient S3 permissions")
    else:
        print("  No buckets available to test")
    
except NoCredentialsError:
    print("✗ No AWS credentials found")
except ClientError as e:
    error_code = e.response['Error']['Code']
    error_msg = e.response['Error']['Message']
    print(f"✗ AWS Error: {error_code}")
    print(f"  Message: {error_msg}")
except Exception as e:
    print(f"✗ Unexpected error: {str(e)}")

print()
print("=" * 60)
print("RECOMMENDATIONS:")
print("=" * 60)
print()
print("If you see 'SignatureDoesNotMatch' errors:")
print("1. Verify your AWS credentials are correct")
print("2. Check if credentials were recently regenerated")
print("3. Ensure your system clock is synchronized")
print("4. Verify credentials have S3 upload permissions")
print()
print("To set credentials on Windows (PowerShell):")
print("  $env:AWS_ACCESS_KEY_ID = 'your-access-key'")
print("  $env:AWS_SECRET_ACCESS_KEY = 'your-secret-key'")
print()
print("Or create a .env file in the project root with:")
print("  AWS_ACCESS_KEY_ID=your-access-key")
print("  AWS_SECRET_ACCESS_KEY=your-secret-key")
print("=" * 60)
