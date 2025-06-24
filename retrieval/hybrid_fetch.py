from azure.cosmos import CosmosClient
from azure.storage.blob import BlobServiceClient
import json, os

cosmos = CosmosClient(os.getenv('COSMOS_URI'), os.getenv('COSMOS_KEY'))
container = cosmos.get_database_client("BillingDB").get_container_client("Records")

blob_client = BlobServiceClient.from_connection_string(os.getenv('BLOB_CONN_STRING'))
archive_container = blob_client.get_container_client("billing-archive")

def get_billing_record(record_id, partition_key):
    try:
        return container.read_item(record_id, partition_key)
    except:
        blob = archive_container.get_blob_client(f"{record_id}.json").download_blob()
        return json.loads(blob.readall())
