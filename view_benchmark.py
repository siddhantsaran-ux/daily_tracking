"""
View and fetch benchmark data from Snowflake.
"""
import os
import pandas as pd
from .snowflake_connector import create_snowflake_write_engine
from .data_processor import convert_to_parquet_compatible


def view_benchmark_from_snowflake(config, benchmark_name=None, platform=None, 
                                  user_type=None, metric_name=None, model_name=None, 
                                  value_name=None, created_at=None, limit=None):
    """
    View benchmark data from Snowflake without saving locally.
    
    Args:
        config (dict): Configuration dictionary
        benchmark_name (str, optional): Filter by benchmark name
        platform (str, optional): Filter by platform
        user_type (str, optional): Filter by user type
        metric_name (str, optional): Filter by metric name
        model_name (str, optional): Filter by model name
        value_name (str, optional): Filter by value name
        created_at (str, optional): Filter by created_at timestamp/date
        limit (int, optional): Limit number of rows returned
    
    Returns:
        pd.DataFrame: Filtered benchmark data
    """
    print("VIEWING BENCHMARK DATA FROM SNOWFLAKE")
    
    full_table_name = f"{config['platform_prefix']}_reports.temp_tables.{config['temp_table_name']}"
    
    # Base query
    query = f"SELECT * FROM {full_table_name}"
    conditions = []
    
    # Dynamically add filters
    if benchmark_name:
        conditions.append(f"benchmark_name = '{benchmark_name}'")
    if platform:
        conditions.append(f"platform = '{platform}'")
    if user_type:
        conditions.append(f"user_type = '{user_type}'")
    if metric_name:
        conditions.append(f"metric_name = '{metric_name}'")
    if model_name:
        conditions.append(f"model_name = '{model_name}'")
    if value_name:
        conditions.append(f"value_name = '{value_name}'")
    if created_at:
        conditions.append(f"created_at = '{created_at}'")  # exact match; can also use >=, <= if needed
    
    # Append WHERE clause if needed
    if conditions:
        query += " WHERE " + " AND ".join(conditions)
    
    if limit:
        query += f" LIMIT {limit}"
    
    # Execute the query
    try:
        engine = create_snowflake_write_engine(config)
        print(f"\n Executing query with filters:")
        
        for param_name, param_value in {
            "Benchmark": benchmark_name,
            "Platform": platform,
            "User Type": user_type,
            "Metric": metric_name,
            "Model": model_name,
            "Value": value_name,
            "Created At": created_at,
            "Limit": limit
        }.items():
            if param_value:
                print(f"  - {param_name}: {param_value}")
        
        df = pd.read_sql(query, engine)
        
        print(f"\n Retrieved {len(df)} rows")
        
        if len(df) > 0:
            print("\n Sample data (first 5 rows):")
            # print(df.head().to_string())
        
        print("VIEW COMPLETE")
        return df
    
    except Exception as e:
        print(f"\n Error retrieving data: {e}")
        return None


def fetch_benchmark_from_snowflake(config, model_name=None, output_path=None):
    """
    Fetch the benchmark table from Snowflake and save it locally as parquet.

    Args:
        config (dict): Configuration dictionary
        model_name (str, optional): Model name to filter data
        output_path (str, optional): Path where the parquet file should be saved

    Returns:
        tuple: (pd.DataFrame, str) - Downloaded dataframe and file path
    """
    print("FETCHING BENCHMARK TABLE FROM SNOWFLAKE")
    
    if model_name:
        print(f"  Filtering by model_name: {model_name}")

    # Import benchmark_fetch_path from config
    # from benchmark.config_bench import benchmark_fetch_path

    full_table_name = f"{config['platform_prefix']}_reports.temp_tables.{config['temp_table_name']}"

    # Set default output path if not provided
    # if output_path is None:
    #     output_path = benchmark_fetch_path
    if output_path is None:
        print('output_path is None')
        return 
    else:
        # Check if output_path is a directory
        if os.path.isdir(output_path) or output_path.endswith('/'):
            output_path = os.path.join(output_path, 'benchmark_table.parquet')
        elif not output_path.endswith('.parquet'):
            output_path = output_path + '.parquet'

    # Fetch data from Snowflake
    try:
        print(f"\n Step 1: Connecting to Snowflake...")
        engine = create_snowflake_write_engine(config)

        print(f" Step 2: Fetching data from table: {full_table_name}")

        # Build query with optional model_name filter
        if model_name:
            query = f"SELECT * FROM {full_table_name} WHERE model_name = '{model_name}'"
        else:
            query = f"SELECT * FROM {full_table_name}"

        df = pd.read_sql(query, engine)
        print(f" Successfully fetched {len(df)} rows and {len(df.columns)} columns")

    except Exception as e:
        print(f" Error fetching data from Snowflake: {e}")
        return None, None

    # Save data locally as parquet
    try:
        print(f"\n Step 3: Saving data as parquet")
        print(f"  Output path: {output_path}")

        # Create directory if it doesn't exist
        output_dir = os.path.dirname(output_path)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)
            print(f"  Created directory: {output_dir}")

        # Convert and save as parquet
        df_compatible = convert_to_parquet_compatible(df)
        df_compatible.to_parquet(
            output_path,
            index=False,
            engine='pyarrow',
            compression='snappy'
        )

        file_size = os.path.getsize(output_path) / 1024**2
        print(f" Successfully saved parquet file")
        print(f" File location: {output_path}")

    except Exception as e:
        print(f" Error saving file: {e}")
        return  None

    return output_path