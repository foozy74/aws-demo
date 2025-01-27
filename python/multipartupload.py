import boto3
import logging
from botocore.exceptions import BotoCoreError, ClientError

# aws s3api list-multipart-uploads --bucket magagm-multipart
# aws s3api list-parts --bucket magagm-multipart --key 1GB.zip --upload-id vjL5I.YqRF1Vs1KZgyVMRUx2OhCECqcz5pPkAwNIJ.SGVn0S__Db0RD7nBLVkUGX4HLcek7n57LepJhXEsOk4MhjFRSdHRTzQ5s.4b.ZV1_OTgd_652uRAsofTw8JlUd

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize the S3 client with timeouts and retries
s3 = boto3.client(
    's3',
    config=boto3.session.Config(
        retries={'max_attempts': 10, 'mode': 'standard'},
        connect_timeout=300,
        read_timeout=300
    )
)

# Specify the bucket name, key (file name in S3), and the file to upload
bucket_name = 'magagm-website'
key = '1GB.zip'
file_path = r'C:\Users\magagm\Documents\Documents\Demonstrations\multipart\1GB.zip'

# Initiate the multipart upload
try:
    response = s3.create_multipart_upload(Bucket=bucket_name, Key=key)
    upload_id = response['UploadId']
    logger.info(f'Upload ID: {upload_id}')
except (BotoCoreError, ClientError) as e:
    logger.error(f'Failed to initiate multipart upload: {e}')
    raise

# Function to upload parts
def upload_part(file_path, part_number, upload_id, chunk_size=100 * 1024 * 1024):
    try:
        with open(file_path, 'rb') as f:
            f.seek(part_number * chunk_size)  # Move to the start of the part
            data = f.read(chunk_size)  # Read the chunk of data
            if not data:
                return None
            response = s3.upload_part(
                Bucket=bucket_name,
                Key=key,
                PartNumber=part_number + 1,
                UploadId=upload_id,
                Body=data
            )
            logger.info(f'Uploaded part {part_number + 1}')
            return {'PartNumber': part_number + 1, 'ETag': response['ETag']}
    except (BotoCoreError, ClientError) as e:
        logger.error(f'Failed to upload part {part_number + 1}: {e}')
        raise

# Upload parts
chunk_size = 100 * 1024 * 1024  # 100 MB
parts = []
part_number = 0

while True:
    part = upload_part(file_path, part_number, upload_id, chunk_size)
    if part:
        parts.append(part)
        part_number += 1
    else:
        break

# Complete the multipart upload
try:
    response = s3.complete_multipart_upload(
        Bucket=bucket_name,
        Key=key,
        MultipartUpload={'Parts': parts},
        UploadId=upload_id
    )
    logger.info('Multipart upload completed successfully.')
    logger.info(response)
except (BotoCoreError, ClientError) as e:
    logger.error(f'Failed to complete multipart upload: {e}')
    raise
