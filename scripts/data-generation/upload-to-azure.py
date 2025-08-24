#!/usr/bin/env python3
"""
Upload Sample Data to Azure Storage
"""

import os
from azure.storage.filedatalake import DataLakeServiceClient
import json

def upload_to_azure():
    """Upload sample data to Azure Storage"""
    print("Uploading sample data to Azure Storage...")
    
    try:
        # Load Azure configuration
        with open('../config/azure-config-dev.json', 'r') as f:
            config = json.load(f)
        
        # Get storage account key (you'll need to set this up)
        storage_account_name = config['storageAccount']
        storage_account_key = os.getenv('AZURE_STORAGE_KEY')  # Set this environment variable
        
        if not storage_account_key:
            print("❌ AZURE_STORAGE_KEY environment variable not set")
            print("Please set your storage account key as environment variable")
            return False
        
        # Create DataLakeServiceClient
        service_client = DataLakeServiceClient(
            account_url=f"https://{storage_account_name}.dfs.core.windows.net",
            credential=storage_account_key
        )
        
        # Get file system client
        file_system_client = service_client.get_file_system_client("sample-data")
        
        # Upload each sample file
        sample_files = ['customers.csv', 'products.csv', 'orders.csv', 'order_items.csv']
        
        for file_name in sample_files:
            local_path = f"../sample-data/{file_name}"
            remote_path = file_name
            
            if os.path.exists(local_path):
                # Upload file
                file_client = file_system_client.get_file_client(remote_path)
                
                with open(local_path, 'rb') as f:
                    file_client.upload_data(f, overwrite=True)
                
                print(f"✓ Uploaded {file_name} to Azure Storage")
            else:
                print(f"⚠ File not found: {local_path}")
        
        print("🎉 All files uploaded successfully to Azure Storage!")
        return True
        
    except Exception as e:
        print(f"❌ Upload error: {e}")
        return False

if __name__ == "__main__":
    upload_to_azure()