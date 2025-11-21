import os
import pandas as pd

from benchmark.snowflake_connector import (
    create_snowflake_read_connector,
    create_snowflake_write_engine
)

def upload_benchmark_to_snowflake(config, parquet_path=None, verify_upload=True):
    """
    Upload benchmark table to Snowflake with smart upsert functionality.
    Uses composite key matching and soft deletes via is_active flag.

    Now matches only on composite key (match_col logic commented for future use).
    """

    print("STARTING BENCHMARK UPLOAD TO SNOWFLAKE")

#     # Import benchmark_output from config
#     from benchmark.config_bench import benchmark_output

#     # Use default path if not provided
    if parquet_path is None:
        print('parquet_path is None')
        return 
        # parquet_path = benchmark_output

    full_table_name = f"{config['platform_prefix']}_reports.temp_tables.{config['temp_table_name']}"

    # Composite key columns
    composite_key_cols = ["model_name", "platform", "user_type", "metric_name", "value_name", "benchmark_name"]

    # match_col = "benchmark_id"   # ← COMMENTED: Future use

    # Step 1: Read parquet file
    print(f"\n Step 1: Reading parquet file from {parquet_path}")
    try:
        if not os.path.exists(parquet_path):
            print(f" Error: Parquet file not found at path: {parquet_path}")
            return None

        df_upload = pd.read_parquet(parquet_path)
        print(f" Successfully read {len(df_upload)} rows")
        print(f"  Columns: {df_upload.columns.tolist()}")

    except Exception as e:
        print(f" Error reading parquet file: {e}")
        return None

    # Validate composite key columns
    missing_cols = [col for col in composite_key_cols if col not in df_upload.columns]
    # + [match_col]  # ← COMMENTED: Future use

    if missing_cols:
        print(f" Error: Missing required columns: {missing_cols}")
        print(f" Available columns: {df_upload.columns.tolist()}")
        return None

    # Ensure is_active column exists
    if "is_active" not in df_upload.columns:
        df_upload["is_active"] = True
        print(" Added 'is_active' column with default value TRUE")

    # Step 2: Initialize Snowflake connections
    print(f"\n Step 2: Initializing Snowflake connections")
    try:
        con = create_snowflake_read_connector(config)
        engine = create_snowflake_write_engine(config)
        cursor = con.cursor()
        print(" Snowflake connections established")
    except Exception as e:
        print(f" Error establishing connections: {e}")
        return None

    # Step 3: Check if table exists
    print(f"\n Step 3: Checking if table exists")
    check_table_query = f"""
        SELECT COUNT(*) as table_count
        FROM {config['snowflake_database']}.information_schema.tables
        WHERE table_schema = '{config['snowflake_schema'].upper()}'
        AND table_name = '{config['temp_table_name'].upper()}'
    """

    try:
        result = pd.read_sql(check_table_query, engine)
        table_exists = result.iloc[0]["table_count"] > 0
        print(f" Table {'exists' if table_exists else 'does not exist'}")
    except Exception as e:
        print(f" Error checking table: {e}")
        table_exists = False

    # Step 4: Process matching rows based on composite key only
    rows_to_deactivate = []

    if table_exists:
        print(f"\n Step 4: Checking for matching rows based ONLY on composite key")

        try:
            fetch_query = f"""
                SELECT *
                FROM {full_table_name}
                WHERE is_active = TRUE
            """
            df_existing = pd.read_sql(fetch_query, engine)
            print(f"  Fetched {len(df_existing)} active rows from Snowflake")

            if len(df_existing) > 0:
                # Create composite keys
                df_upload["_composite_key"] = df_upload[composite_key_cols].astype(str).agg("||".join, axis=1)
                df_existing["_composite_key"] = df_existing[composite_key_cols].astype(str).agg("||".join, axis=1)

                # FUTURE LOGIC:
                # new_benchmark_id = new_row[match_col]

                matches_found = 0

                for idx, new_row in df_upload.iterrows():
                    composite_key = new_row["_composite_key"]

                    matching_rows = df_existing[df_existing["_composite_key"] == composite_key]

                    if len(matching_rows) > 0:
                        matches_found += len(matching_rows)

                        # OLD exact match check (commented):
                        # exact_matches = matching_rows[matching_rows[match_col] == new_benchmark_id]
                        # if len(exact_matches) > 0:

                        for _, old_row in matching_rows.iterrows():
                            rows_to_deactivate.append({
                                col: old_row[col] for col in composite_key_cols
                                # + [match_col]  # ← COMMENTED
                            })

                df_upload.drop(columns=["_composite_key"], inplace=True)

                print(f"  Found {matches_found} composite key matches")
                print(f"  Rows to deactivate: {len(rows_to_deactivate)}")

                # Deactivate
                if rows_to_deactivate:
                    print(f"  Deactivating {len(rows_to_deactivate)} row(s)...")

                    for row_info in rows_to_deactivate:
                        where_conditions = " AND ".join([
                            f"{col} = '{row_info[col]}'" for col in composite_key_cols
                            # + [match_col]   # ← COMMENTED
                        ])

                        update_query = f"""
                            UPDATE {full_table_name}
                            SET is_active = FALSE
                            WHERE {where_conditions}
                            AND is_active = TRUE
                        """

                        cursor.execute(update_query)

                    con.commit()
                    print(" Successfully deactivated matching rows")

                else:
                    print(" No rows to deactivate (no composite matches)")
            else:
                print("  No active rows in Snowflake, skipping match check")

        except Exception as e:
            print(f" Error during matching/deactivation: {e}")
            con.close()
            return None

    else:
        print("\n Step 4: Skipped (table doesn't exist)")

    # Step 5: Upload new rows
    print(f"\n Step 5: Uploading {len(df_upload)} new rows to Snowflake")
    try:
        df_upload.to_sql(
            name=config["temp_table_name"],
            con=engine,
            index=False,
            if_exists="append",
            chunksize=100000,
        )
        print(f" Successfully uploaded {len(df_upload)} rows to '{full_table_name}'")
    except Exception as e:
        print(f" Error uploading: {e}")
        con.close()
        return None

        con.close()
    print("BENCHMARK UPLOAD COMPLETE")

    return None























# """
# Upload benchmark table to Snowflake with smart upsert functionality.
# """
# import os
# import pandas as pd
# from benchmark.snowflake_connector import create_snowflake_read_connector, create_snowflake_write_engine


# def upload_benchmark_to_snowflake(config, parquet_path=None, verify_upload=True):
#     """
#     Upload benchmark table to Snowflake with smart upsert functionality.
#     Uses composite key matching and soft deletes via is_active flag.

#     Args:
#         config (dict): Configuration dictionary
#         parquet_path (str, optional): Path to parquet file
#         verify_upload (bool, optional): Verify upload with counts

#     Returns:
#         pd.DataFrame: Verification results if verify_upload=True, else None
#     """
#     print("STARTING BENCHMARK UPLOAD TO SNOWFLAKE")

#     # Import benchmark_output from config
#     from benchmark.config_bench import benchmark_output

#     # Use default path if not provided
#     if parquet_path is None:
#         parquet_path = benchmark_output

#     full_table_name = f"{config['platform_prefix']}_reports.temp_tables.{config['temp_table_name']}"

#     # Composite key columns
#     composite_key_cols = ["model_name", "platform", "user_type", "metric_name", "value_name", "benchmark_name"]
#     # match_col = "benchmark_id"

#     # Step 1: Read parquet file
#     print(f"\n Step 1: Reading parquet file from {parquet_path}")
#     try:
#         if not os.path.exists(parquet_path):
#             print(f" Error: Parquet file not found at path: {parquet_path}")
#             return None

#         df_upload = pd.read_parquet(parquet_path)
#         print(f" Successfully read {len(df_upload)} rows")
#         print(f"  Columns: {df_upload.columns.tolist()}")
#     except Exception as e:
#         print(f" Error reading parquet file: {e}")
#         return None

#     # Validate that composite key columns exist
#     # missing_cols = [col for col in composite_key_cols + [match_col] if col not in df_upload.columns]
#     missing_cols = [col for col in composite_key_cols  if col not in df_upload.columns]
    
#     if missing_cols:
#         print(f" Error: Missing required columns: {missing_cols}")
#         print(f" Available columns: {df_upload.columns.tolist()}")
#         return None

#     # Ensure is_active column exists
#     if 'is_active' not in df_upload.columns:
#         df_upload['is_active'] = True
#         print(f" Added 'is_active' column with default value TRUE")

#     # Step 2: Initialize Snowflake connections
#     print(f"\n Step 2: Initializing Snowflake connections")
#     try:
#         con = create_snowflake_read_connector(config)
#         engine = create_snowflake_write_engine(config)
#         cursor = con.cursor()
#         print(f" Snowflake connections established")
#     except Exception as e:
#         print(f" Error establishing connections: {e}")
#         return None

#     # Step 3: Check if table exists
#     print(f"\n Step 3: Checking if table exists")
#     check_table_query = f"""
#         SELECT COUNT(*) as table_count
#         FROM {config['snowflake_database']}.information_schema.tables
#         WHERE table_schema = '{config['snowflake_schema'].upper()}'
#         AND table_name = '{config['temp_table_name'].upper()}'
#     """

#     try:
#         result = pd.read_sql(check_table_query, engine)
#         table_exists = result.iloc[0]['table_count'] > 0
#         print(f" Table {'exists' if table_exists else 'does not exist'}")
#     except Exception as e:
#         print(f" Error checking table: {e}")
#         table_exists = False

#     # Step 4: Process matching rows if table exists
#     rows_to_deactivate = []

#     if table_exists:
#         print(f"\n Step 4: Checking for matching rows based on composite key")

#         try:
#             # Fetch existing active data
#             fetch_query = f"""
#                 SELECT *
#                 FROM {full_table_name}
#                 WHERE is_active = TRUE
#             """
#             df_existing = pd.read_sql(fetch_query, engine)
#             print(f"  Fetched {len(df_existing)} active rows from Snowflake")

#             if len(df_existing) > 0:
#                 # Create composite key
#                 df_upload['_composite_key'] = df_upload[composite_key_cols].astype(str).agg('||'.join, axis=1)
#                 df_existing['_composite_key'] = df_existing[composite_key_cols].astype(str).agg('||'.join, axis=1)

#                 # Find matches
#                 matches_found = 0
#                 for idx, new_row in df_upload.iterrows():
#                     composite_key = new_row['_composite_key']
#                     new_benchmark_id = new_row[match_col]

#                     matching_rows = df_existing[df_existing['_composite_key'] == composite_key]

#                     if len(matching_rows) > 0:
#                         exact_matches = matching_rows[matching_rows[match_col] == new_benchmark_id]

#                         if len(exact_matches) > 0:
#                             matches_found += 1
#                             for _, old_row in exact_matches.iterrows():
#                                 rows_to_deactivate.append({
#                                     col: old_row[col] for col in composite_key_cols + [match_col]
#                                 })

#                 # Remove temporary composite key column
#                 df_upload = df_upload.drop(columns=['_composite_key'])

#                 print(f"  Found {matches_found} composite key + benchmark_id matches")
#                 print(f"  Total rows to deactivate: {len(rows_to_deactivate)}")

#                 # Deactivate matching rows
#                 if rows_to_deactivate:
#                     print(f"  Deactivating {len(rows_to_deactivate)} row(s)...")

#                     for row_info in rows_to_deactivate:
#                         where_conditions = " AND ".join([
#                             f"{col} = '{row_info[col]}'" for col in composite_key_cols + [match_col]
#                         ])

#                         update_query = f"""
#                             UPDATE {full_table_name}
#                             SET is_active = FALSE
#                             WHERE {where_conditions}
#                             AND is_active = TRUE
#                         """

#                         cursor.execute(update_query)

#                     con.commit()
#                     print(f" Successfully deactivated matching rows")
#                 else:
#                     print(f" No rows to deactivate (no exact matches found)")
#             else:
#                 print(f"  No active rows in Snowflake, skipping match check")

#         except Exception as e:
#             print(f" Error during matching/deactivation: {e}")
#             con.close()
#             return None
#     else:
#         print(f"\n Step 4: Skipped (table doesn't exist)")

#     # Step 5: Upload data
#     print(f"\n Step 5: Uploading {len(df_upload)} new rows to Snowflake")
#     try:
#         df_upload.to_sql(
#             name=config['temp_table_name'],
#             con=engine,
#             index=False,
#             if_exists='append',
#             chunksize=100000
#         )
#         print(f" Successfully uploaded {len(df_upload)} rows to '{full_table_name}'")
#     except Exception as e:
#         print(f" Error uploading: {e}")
#         con.close()
#         return None

#     # Step 6: Verify upload
#     verification_result = None
#     if verify_upload:
#         print(f"\n✅ Step 6: Verifying upload")
#         try:
#             verify_query = f"""
#                 SELECT 
#                     {', '.join(composite_key_cols)},
#                     {match_col},
#                     is_active,
#                     COUNT(*) as row_count
#                 FROM {full_table_name}
#                 GROUP BY {', '.join(composite_key_cols)}, {match_col}, is_active
#                 ORDER BY {', '.join(composite_key_cols)}, {match_col}, is_active DESC
#             """

#             verification_result = pd.read_sql(verify_query, engine)
#             print(f" Verification complete")
#             print(f"\n Summary of rows in Snowflake:")
#             print(f"  Total active rows: {verification_result[verification_result['is_active'] == True]['row_count'].sum()}")
#             print(f"  Total inactive rows: {verification_result[verification_result['is_active'] == False]['row_count'].sum()}")
#         except Exception as e:
#             print(f" Error during verification: {e}")

#     con.close()

#     print("BENCHMARK UPLOAD COMPLETE")

#     return verification_result