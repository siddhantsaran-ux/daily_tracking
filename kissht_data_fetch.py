import pandas as pd
import numpy as np
import pickle
import json
import datetime
import os
import snowflake.connector
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL
import boto3
import shutil
import re

# Global variables
platform_prefix_name = "kissht"
your_name = "your_name"
email_id = "(optional)"
temp_table_name = f"{your_name.lower()}_temp_data"
your_s3_folder_name = f"Mathlogic-model-data/{your_name.lower()}/temp_file"
s3_bucket = 'dwh-analytics-dump-new'


def fetch_snowflake_kissht_data(start_date,end_date,local_folder, add_prefix=""):
    
    query= 
    def snowflake_read_connector(input_role, input_warehouse, input_database=None, input_schema=None):
        """Create Snowflake read connection."""
        with open("/home/ubuntu/temp_data/credentials/KISSHT_ANALYTICS_AUTOMATION_rsa_key.p8.der", "rb") as key_file:
            private_key = key_file.read()
        
        connection_params = {
            "user": "KISSHT_ANALYTICS_AUTOMATION",
            "private_key": private_key,
            "account": "nx43209.ap-south-1.aws",
            "role": input_role,
            "warehouse": input_warehouse,
            "ocsp_fail_open": False
        }
     
        if input_database:
            connection_params["database"] = input_database
        if input_schema:
            connection_params["schema"] = input_schema
     
        return snowflake.connector.connect(**connection_params)

    def snowflake_write_engine(input_role, input_warehouse, input_database=None, input_schema=None):
        """Create Snowflake write engine."""
        with open("/home/ubuntu/temp_data/credentials/KISSHT_ANALYTICS_AUTOMATION_rsa_key.p8.der", "rb") as key_file:
            private_key = key_file.read()
     
        url_params = {
            "user": "KISSHT_ANALYTICS_AUTOMATION",
            "account": "nx43209.ap-south-1.aws",
            "role": input_role,
            "warehouse": input_warehouse
        }
     
        if input_database:
            url_params["database"] = input_database
        if input_schema:
            url_params["schema"] = input_schema
     
        engine = create_engine(
            URL(**url_params),
            connect_args={"private_key": private_key, "ocsp_fail_open": False}
        )
     
        return engine

    def snowflake_fetch_s3(query, local_folder, conn, s3_file_name, add_prefix=""):
        """Execute query and download results from S3."""
        query = query.replace(';', '')
        
        try:
            cursor = conn.cursor()
            sf_file_path = f'{s3_file_name}'
            
            copy_query = f"""
                COPY INTO @RING_REPORTS.ANALYTICS.DATASCIENCE_ANALYTICS_DUMP/{sf_file_path}
                FROM ({query})
                FILE_FORMAT = (TYPE = PARQUET)
                HEADER = TRUE
                OVERWRITE = TRUE;
            """
            
            cursor.execute(copy_query)
        except Exception as e:
            print(f"Error executing query: {e}")
            raise
        
        s3 = boto3.client('s3')
        os.makedirs(local_folder, exist_ok=True)
        
        response = s3.list_objects_v2(Bucket=s3_bucket, Prefix=s3_file_name)
        
        if 'Contents' in response:
            for obj in response['Contents']:
                s3_file_key = obj['Key']
                local_file_path = os.path.join(
                    local_folder, 
                    f"{platform_prefix_name}_base_{add_prefix}_" + os.path.basename(s3_file_key)
                )
                
                s3.download_file(s3_bucket, s3_file_key, local_file_path)
                s3.delete_object(Bucket=s3_bucket, Key=s3_file_key)
        
        print("Data successfully downloaded to local directory!")
    
    # Main execution
    con = snowflake_read_connector(
        input_role='KISSHT_ANALYTICS_PROGRAMATIC_ACCESS_EDIT_ONLY_ROLE',
        input_warehouse='KISSHT_ANALYTICS_PROGRAMATIC_ACCESS_EDIT_ONLY_ROLE_WH'
    )
    
    engine = snowflake_write_engine(
        input_role='KISSHT_ANALYTICS_PROGRAMATIC_ACCESS_EDIT_ONLY_ROLE',
        input_warehouse='KISSHT_ANALYTICS_PROGRAMATIC_ACCESS_EDIT_ONLY_ROLE_WH',
        input_database='KISSHT_REPORTS',
        input_schema='TEMP_TABLES'
    )
    
    try:
        snowflake_fetch_s3(query, local_folder, con, your_s3_folder_name, add_prefix)
    finally:
        con.close()


# Example usage:
if __name__ == "__main__":
    base_query = f"""
    select c.*
    from {platform_prefix_name}_reports.temp_tables.{temp_table_name} a
    inner join
    kissht_source.bureau_scrub.equifax_output c
    on concat('#',a.user_reference_number) = c.reference_no
    """
    
    fetch_snowflake_kissht_data(
        query=base_query,
        local_folder=local_folder
    )