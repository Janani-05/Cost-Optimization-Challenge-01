from azure.cosmos import CosmosClient
from azure.storage.blob import BlobServiceClient
import datetime, json, os

def archive_old_records():
    cosmos = CosmosClient(os.getenv('COSMOS_URI'), os.getenv('COSMOS_KEY'))
    container = cosmos.get_database_client("BillingDB").get_container_client("Records")

    blob_client = BlobServiceClient.from_connection_string(os.getenv('BLOB_CONN_STRING'))
    archive_container = blob_client.get_container_client("billing-archive")

    cutoff = datetime.datetime.utcnow() - datetime.timedelta(days=90)
    query = f"SELECT * FROM c WHERE c.date < '{cutoff.isoformat()}'"

    for record in container.query_items(query, enable_cross_partition_query=True):
        blob_name = f"{record['id']}.json"
        archive_container.upload_blob(name=blob_name, data=json.dumps(record), overwrite=True)
        container.delete_item(record['id'], partition_key=record['partitionKey'])
