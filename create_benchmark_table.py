import pandas as pd
from benchmark.data_processor import (
    process_kissht_data, process_ring_data,
    merge_and_transform_data, convert_to_parquet_compatible
)
from benchmark.utils import create_benchmark

# from benchmark.kissht_data_fetch import *
# from benchmark.ring_data_fetch import *


def create_benchmark_table(
    spark,
    config,
    metric_dict,
    meta_dict,
    benchmark_start_date,
    benchmark_end_date,
    benchmark_end_period,
    data_source ='other',
    platforms=None,
    user_types=None,
    model_name=None,
    benchmark_name=None,
    save_intermediate=True,
    bench_path = '/home/manu.chandra/daily_tracking/'
):
    """
    Create benchmark from L1 data sources.

    Args:
        spark: Spark session
        config (dict): MODEL_CONFIG[model_name]
        metric_dict (dict)
        meta_dict (dict)
        benchmark_start_date / end_date / end_period: Optional metadata
        platforms (list)
        user_types (list)
        model_name (str)
        benchmark_name (str)
        save_intermediate (bool)
    """
    
    print("STARTING BENCHMARK CREATION")

    # use values from model config
    platforms = platforms or config.get("platforms", [])
    user_types = user_types or config.get("user_types", [])

    # all required paths come from MODEL_CONFIG
    kissht_feat_path   = config["kissht_feat_path"]
    kissht_out_path    = config["kissht_out_path"]
    kissht_map_path    = config["kissht_map_path"]
    kissht_prod_path   = config["kissht_prod_path"]

    ring_feat_path     = config["ring_feat_path"]
    ring_out_path      = config["ring_out_path"]
    ring_map_path      = config["ring_map_path"]
    ring_prod_path     = config["ring_prod_path"]

    benchmark_input    = bench_path+'bench_input.parquet'
    benchmark_output   = bench_path+'bench_output.parquet'

    if data_source == 'other':
        df_k1 = process_kissht_data(
            spark,
            kissht_feat_path, kissht_out_path,
            kissht_map_path, kissht_prod_path
        )

        df_r1 = process_ring_data(
            spark,
            ring_feat_path, ring_out_path,
            ring_map_path, ring_prod_path
        )

        df_final = merge_and_transform_data(df_k1, df_r1)
    elif  data_source == 'snowflake':
        print('fetching data from snowflake for benchmark creation')
        return
    # Deduplication
    print("\n Deduplicating merged data...")
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    initial_count = df_final.count()

    w = Window.partitionBy("id").orderBy(F.desc("created_at"))
    df_final = (
        df_final
        .withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

    print(f"Removed {initial_count - df_final.count()} duplicates")

    # Save intermediate
    if save_intermediate:
        print(f"\n Saving intermediate data → {benchmark_input}")
        df_final.write.mode("overwrite").parquet(benchmark_input)


    benchmark_list = []

    print("\n Creating benchmark tables ...")

    for platform in platforms:
        for user_type in user_types:
            print(f"  → Platform: {platform} | User Type: {user_type}")

            df_bench = create_benchmark(
                df_final,
                platform,
                user_type,
                model_name,
                metric_dict,
                meta_dict,
                benchmark_name,
                benchmark_start_time=benchmark_start_date,
                benchmark_end_time=benchmark_end_date,
            )

            benchmark_list.append(df_bench)

    final_df = pd.concat(benchmark_list, axis=0, ignore_index=True)


    print("\n Cleaning duplicates ...")

    key_columns = ["model_name", "platform", "user_type", "metric_name", "value_name"]

    before = len(final_df)
    final_df["_nulls"] = final_df.isnull().sum(axis=1)
    final_df = (
        final_df.sort_values("_nulls")
        .drop_duplicates(subset=key_columns, keep="first")
        .drop(columns=["_nulls"])
    )
    after = len(final_df)

    print(f"Removed {before - after} duplicate rows")


    final_df = convert_to_parquet_compatible(final_df)

    # Save output
    print(f"\n Saving final benchmark → {benchmark_output}")
    final_df.to_parquet(
        benchmark_output,
        index=False,
        engine="pyarrow",
        compression="snappy"
    )

    print(f"Benchmark creation COMPLETE")
    print(f"Output Shape: {final_df.shape}")

    return final_df