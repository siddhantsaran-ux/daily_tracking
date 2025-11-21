"""
Snowflake connection utilities for BenchmarkManager.
"""
import snowflake.connector
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL


def create_snowflake_read_connector(config):
    """
    Create a Snowflake connection for reading data.
    
    Args:
        config (dict): Configuration dictionary with Snowflake credentials
        
    Returns:
        snowflake.connector.connection: Snowflake connection object
    """
    with open(config['private_key_path'], "rb") as key_file:
        private_key = key_file.read()
    
    connection_params = {
        "user": config['snowflake_user'],
        "private_key": private_key,
        "account": config['snowflake_account'],
        "role": config['snowflake_role'],
        "warehouse": config['snowflake_warehouse'],
        "database": config['snowflake_database'],
        "schema": config['snowflake_schema'],
        "ocsp_fail_open": False
    }
    
    return snowflake.connector.connect(**connection_params)


def create_snowflake_write_engine(config):
    """
    Create a SQLAlchemy engine for writing data to Snowflake.
    
    Args:
        config (dict): Configuration dictionary with Snowflake credentials
        
    Returns:
        sqlalchemy.engine.Engine: SQLAlchemy engine
    """
    with open(config['private_key_path'], "rb") as key_file:
        private_key = key_file.read()
    
    url_params = {
        "user": config['snowflake_user'],
        "account": config['snowflake_account'],
        "role": config['snowflake_role'],
        "warehouse": config['snowflake_warehouse'],
        "database": config['snowflake_database'],
        "schema": config['snowflake_schema']
    }
    
    engine = create_engine(
        URL(**url_params),
        connect_args={"private_key": private_key, "ocsp_fail_open": False}
    )
    
    return engine