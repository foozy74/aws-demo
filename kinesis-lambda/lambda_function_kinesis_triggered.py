import json
import base64
import boto3
import logging
import time  

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client('s3')

# List to store records
all_records = []

def write_to_s3(records):
    try:
        # Specify your S3 bucket and key for the single file
        key = 'weather-data/all_records.json'

        # Store all records in S3
        s3_client.put_object(
            Bucket='futurexskills',
            Key=key,
            Body=json.dumps(records)
        )

        # Log success
        logger.info(f"Successfully wrote {len(records)} records to S3. Key: {key}")

    except Exception as e:
        # Log error
        logger.error(f"Error writing to S3: {e}")

def lambda_handler(event, context):
    global all_records
    try:
        for record in event['Records']:
            try:
                # Process the data from the Kinesis record
                kinesis_data = record['kinesis']['data']
                
                # Decode base64-encoded data
                decoded_data = base64.b64decode(kinesis_data).decode('utf-8')
                
                # Log the decoded data
                logger.info(f"Decoded Kinesis Data: {decoded_data}")

                # Parse JSON data
                data = json.loads(decoded_data)

                # Example: Extract relevant data
                city = data.get('city', 'UnknownCity')
                timestamp = data.get('timestamp', int(time.time()))
                temperature = data.get('temperature', 0.0)
                humidity = data.get('humidity', 0.0)

                # Format the data
                record_data = {
                    'city': city,
                    'timestamp': timestamp,
                    'temperature': temperature,
                    'humidity': humidity
                }

                # Append the record to the list
                all_records.append(record_data)

            except json.JSONDecodeError as e:
                logger.error(f"Error decoding JSON: {e}")
                continue  # Skip to the next record if there's an error

        # Store all records in S3
        write_to_s3(all_records)

        return {
            'statusCode': 200,
            'body': json.dumps('All records processed and stored in S3')
        }

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps('Internal Server Error')
        }
