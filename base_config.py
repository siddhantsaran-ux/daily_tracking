
# Snowflake settings
private_key_path = '/home/ubuntu/temp_data/credentials/KISSHT_ANALYTICS_AUTOMATION_rsa_key.p8.der'
snowflake_user = 'KISSHT_ANALYTICS_AUTOMATION'
snowflake_account = 'nx43209.ap-south-1.aws'
snowflake_role = 'KISSHT_ANALYTICS_PROGRAMATIC_ACCESS_EDIT_ONLY_ROLE'
snowflake_warehouse = 'KISSHT_ANALYTICS_PROGRAMATIC_ACCESS_EDIT_ONLY_ROLE_WH'
snowflake_database = 'KISSHT_REPORTS'
snowflake_schema = 'TEMP_TABLES'

# Table settings
temp_table_name = 'benchmark_table'
platform_prefix = 'kissht'
unique_column = 'benchmark_name'
bench_out='/home/manu.chandra/daily_tracking/01.benchmark_function_files/temp_data/'



BASE_CONFIG = {
    'private_key_path':private_key_path,
    'snowflake_user':snowflake_user,
    'snowflake_account':snowflake_account,
    'snowflake_role':snowflake_role,
    'snowflake_warehouse':snowflake_warehouse,
    'snowflake_database':snowflake_database,
    'snowflake_schema':snowflake_schema,

    # Table settings
    'temp_table_name': temp_table_name,
    'platform_prefix': platform_prefix,
    'unique_column': unique_column,

    # Optional
    'bench_out': bench_out   # only default storage
}
