import pandas as pd
import time
import json
import kagglehub
import logging
import os
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_kinesis_client():
    """Creates a Kinesis client.
    Assumes credentials and region are configured via environment variables or IAM roles.
    """
    aws_region = os.environ.get('AWS_REGION', 'us-east-1') # Default region if not set
    logger.info(f"Creating Kinesis client in region: {aws_region}")
    try:
        client = boto3.client('kinesis', region_name=aws_region)
        # Quick check to see if client can describe streams (verifies credentials and region vaguely)
        client.list_streams(Limit=1)
        logger.info("Kinesis client created successfully.")
        return client
    except (NoCredentialsError, PartialCredentialsError) as e:
        logger.error(f"AWS credentials not found or incomplete: {e}")
        return None
    except ClientError as e:
        if e.response['Error']['Code'] == 'UnrecognizedClientException':
            logger.error(f"AWS Region '{aws_region}' might be incorrect or Kinesis service is not available there: {e}")
        else:
            logger.error(f"Failed to create Kinesis client due to ClientError: {e}")
        return None
    except Exception as e:
        logger.error(f"An unexpected error occurred while creating Kinesis client: {e}")
        return None

def stream_data(kinesis_client, stream_name, csv_file):
    if not kinesis_client:
        logger.error("Kinesis client is not available. Cannot stream data.")
        return

    try:
        df = pd.read_csv(csv_file)
        
        if 'clean_text' not in df.columns:
            logger.error("Error: 'clean_text' column not found in the CSV.")
            return
        
        df = df.dropna(subset=['clean_text'])
        df = df.tail(1000)  # Sample size

        logger.info(f"Starting to stream {len(df)} records to Kinesis stream: {stream_name}")

        for index, row in df.iterrows():
            message = {
                "id": index,
                "text": row['clean_text'],
                "produced_at": time.time(),
            }
            # Kinesis needs data as bytes, and a partition key
            # Using 'id' as partition key for reasonably good distribution for this example
            partition_key = str(message["id"])
            payload = json.dumps(message).encode('utf-8')

            try:
                response = kinesis_client.put_record(
                    StreamName=stream_name,
                    Data=payload,
                    PartitionKey=partition_key
                )
                logger.info(f"Sent record with ID {message['id']} to Kinesis. SequenceNumber: {response['SequenceNumber']}")
            except ClientError as e:
                logger.error(f"Failed to send record ID {message['id']} to Kinesis: {e}")
                # Depending on the error, you might want to retry or handle it differently
            except Exception as e:
                logger.error(f"An unexpected error occurred while sending record ID {message['id']} to Kinesis: {e}")

            time.sleep(0.1) # Adjusted sleep time for Kinesis (can be tuned)
            
    except FileNotFoundError:
        logger.error(f"Error: The file {csv_file} was not found.")
    except Exception as e:
        logger.error(f"An error occurred during data streaming: {e}", exc_info=True)

if __name__ == "__main__":
    kinesis_stream_name = os.environ.get('KINESIS_STREAM_NAME')
    if not kinesis_stream_name:
        logger.error("KINESIS_STREAM_NAME environment variable not set. Exiting.")
    else:
        kinesis_client = create_kinesis_client()
        if kinesis_client:
            try:
                # Path to the Kaggle dataset CSV
                # In a Fargate/container environment, this dataset might need to be baked into the image
                # or downloaded from S3 at startup. For simplicity, we keep the Kaggle download here.
                # Ensure the container has internet access and kagglehub is configured if using this directly.
                logger.info("Downloading dataset from Kaggle...")
                path = kagglehub.dataset_download("saurabhshahane/twitter-sentiment-dataset")
                csv_path = os.path.join(path, "Twitter_Data.csv")
                logger.info(f"Dataset downloaded to: {csv_path}")

                stream_data(kinesis_client, kinesis_stream_name, csv_path)
            except Exception as e:
                logger.error(f"Failed to download or process dataset: {e}", exc_info=True)
            finally:
                logger.info("Producer finished.")
                # Kinesis client doesn't have a 'close()' method like KafkaProducer