import os
from azure.storage.blob import BlobServiceClient

connection_string = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
container_name = "iot-pipeline-data"

blob_service_client = BlobServiceClient.from_connection_string(connection_string)
container_client = blob_service_client.get_container_client(container_name)

local_folder = "flagged_data.parquet"

for root, dirs, files in os.walk(local_folder):
    for filename in files:
        local_path = os.path.join(root, filename)
        blob_path = os.path.join("flagged_data.parquet", filename).replace("\\", "/")
        with open(local_path, "rb") as data:
            container_client.upload_blob(name=blob_path, data=data, overwrite=True)
        print(f"Uploaded: {blob_path}")

print("Upload complete.")