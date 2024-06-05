from azure.storage.blob.aio import BlobServiceClient
import random
import time
import json
import requests
import asyncio

account_name = 'destorage'
account_url = "https://destorage.blob.core.windows.net"
account_key = "oE3jJxw/UQqGE+rtELTW7Ioq2gvnzfaITDgB+zZrT7iY3ORur2vwsIV7Y/UUSGxiNpQdKZymakcW+AStjDaDvw=="
container_name = "depipelines"


def blob_connection(account_url,account_key,container_name):
    try:
        blob_service_client = BlobServiceClient(account_url=account_url, credential=account_key)
        container_client = blob_service_client.get_container_client(container_name)
        return container_client
    except Exception as e:
        print(f'Error in Blob Connection: {str(e)}')

async def save_data_to_blob_storage(plant_name, data, file_time, plugin_type='Extract'):
    try:
        try:
            blob_name =''
            container_client = blob_connection(account_url,account_key,container_name)
            print("Blob Client Connected")
            if container_client is not None:
                blob_name = f'{plant_name}/{plugin_type}/{plugin_type}_{file_time}_{str(int(time.time()))}_{random.randint(10000000, 99999999)}.json'
                # Save data to Azure Blob Storage
                blob_client = container_client.get_blob_client(blob_name)
                await blob_client.upload_blob(json.dumps(data))
                print(f"File uploaded successfully on Blob {blob_name}!")
                await container_client.close()
        except Exception as e:
            print(f"Blob Upload Error:  '{blob_name}'{str(e)}")
        return
    except:
        pass
    return


async def read_files_from_blob(plant_name, plugin_type='Extract'):
    blob_names = []
    try:
        try:
            container_client = blob_connection(account_url,account_key,container_name)
            print("Blob Client Connected")
            if container_client is not None:
                starts_with = f"{plant_name}/{plugin_type}/"
                blob_list = container_client.list_blobs(name_starts_with=starts_with)
                async for blob in blob_list:
                    blob_names.append(blob.name)
                    print(f"Name: {blob.name}")
                await container_client.close()
        except Exception as e:
            print(f"Blob Read Error: {str(e)}")
        return blob_names
    except:
        pass
    return


async def move_blob(plant, files, plugin_type='Extract'):
    try:
        src_container_client = blob_connection(account_url, account_key, container_name)
        if not src_container_client:
            return
        
        for file in files:
            src_blob_name = f'{plant}/{plugin_type}/{file}'
            dest_blob_name = f'{plant}/Processed/{plugin_type}/{file}'
            print(f"{src_blob_name=}, {dest_blob_name=}")
            
            # Source blob client
            src_blob_client = src_container_client.get_blob_client(src_blob_name)
            
            # Check if the source blob exists
            exists = await src_blob_client.exists()
            if not exists:
                print(f"Source blob does not exist: {src_blob_name}")
                continue
            
            # Destination blob client
            dest_blob_client = src_container_client.get_blob_client(dest_blob_name)
            
            # Start copy operation
            copy_source_url = src_blob_client.url
            print(f"{copy_source_url=}")
            copy_operation = await dest_blob_client.start_copy_from_url(copy_source_url)
            
            # Wait for the copy operation to complete
            properties = await dest_blob_client.get_blob_properties()
            while properties.copy.status == "pending":
                await asyncio.sleep(1)
                properties = await dest_blob_client.get_blob_properties()
            
            if properties.copy.status == "success":
                print(f"Blob copied successfully to {dest_blob_name}")
                
                # Delete the source blob
                await src_blob_client.delete_blob()
                print(f"Source blob {src_blob_name} deleted successfully.")
            else:
                print(f"Failed to copy blob: {properties.copy.status_description}")
            
            src_blob_client.close()
            dest_blob_client.close()
            
        src_container_client.close()
    except Exception as e:
        print(f"Error moving blob: {str(e)}")

async def move_blob_v0(plant, files, plugin_type='Extract'):
    try:
        src_container_client = blob_connection(account_url, account_key, container_name)
        if not src_container_client:
            return
        for file in files:
            src_blob_name = f'{plant}/{plugin_type}/{file}'
            dest_blob_name = f'{plant}/{plugin_type}/Processed/{file}'
            print(f"{src_blob_name=}, {dest_blob_name=}")
            # Source blob client
            src_blob_client = src_container_client.get_blob_client(src_blob_name)
            
            # Destination blob client
            dest_blob_client = src_container_client.get_blob_client(dest_blob_name)
            
            # Start copy operation
            copy_source_url = src_blob_client.url
            print(f"{copy_source_url=}")
            copy_operation = dest_blob_client.start_copy_from_url(copy_source_url)
            
            # Wait for the copy operation to complete
            properties = await dest_blob_client.get_blob_properties()
            while properties.copy.status == "pending":
                await asyncio.sleep(1)
                properties = dest_blob_client.get_blob_properties()
            
            if properties.copy.status == "success":
                print(f"Blob copied successfully to {dest_blob_name}")
                
                # Delete the source blob
                src_blob_client.delete_blob()
                print(f"Source blob {src_blob_name} deleted successfully.")
            else:
                print(f"Failed to copy blob: {properties.copy.status_description}")
    except Exception as e:
        print(f"Error moving blob: {str(e)}")



def read_data_file_blob(blob_name):
    url = f'{account_url}/{container_name}/{blob_name}'
    json_data =None
    response = requests.get(url)
    if response.status_code==200:
        json_data = json.loads(response.text)
    else:
        print(f"File Reading Status Error: {response.text} Status Code: {response.status_code}")
    return json_data
