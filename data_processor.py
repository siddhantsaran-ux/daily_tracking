"""
Data processing functions for KISSHT and RING data.
"""
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import MapType, StringType
import pandas as pd 
import numpy as np 
import json


def process_kissht_data(spark, kissht_feat_path, kissht_out_path, kissht_map_path, kissht_prod_path):
    """
    Process KISSHT data from L1 sources.
    
    Args:
        spark: Spark session
        kissht_feat_path (str): Path to KISSHT features
        kissht_out_path (str): Path to KISSHT output
        kissht_map_path (str): Path to KISSHT mapping
        kissht_prod_path (str): Path to KISSHT production
        
    Returns:
        DataFrame: Processed KISSHT data
    """
    print(" Processing KISSHT Data")
    
    # Read parquet files
    df =  spark.read.parquet(kissht_feat_path)
    df1 = spark.read.parquet(kissht_out_path)
    df2 = spark.read.parquet(kissht_map_path)
    df3 = spark.read.parquet(kissht_prod_path)
    
    df1 = df1.join(df, on='id', how='inner')
    
    w = Window.partitionBy("BUREAU_REFERENCE_NUMBER").orderBy(F.desc("created_at"))
    
    df3_latest = df3 \
        .withColumn("rn", F.row_number().over(w)) \
        .filter(F.col("rn") == 1) \
        .drop("rn", "INPUT_VALUE", "OUTPUT_VALUE", "PLATFORM", "MODEL_VERSION", "BUREAU_VINTAGE") \
        .withColumn('platform', F.lit('KISSHT'))
    
    df_k = df1.join(df3_latest, df1.id == df3_latest.BUREAU_REFERENCE_NUMBER, "inner")
    df_k1 = df_k.toDF(*[c.lower() for c in df_k.columns])
    
    print(" KISSHT data processed successfully")
    return df_k1


def process_ring_data(spark, ring_feat_path, ring_out_path, ring_map_path, ring_prod_path):
    """
    Process RING data from L1 sources.
    
    Args:
        spark: Spark session
        ring_feat_path (str): Path to RING features
        ring_out_path (str): Path to RING output
        ring_map_path (str): Path to RING mapping
        ring_prod_path (str): Path to RING production
        
    Returns:
        DataFrame: Processed RING data
    """
    print(" Processing RING Data")
    
    # Read parquet files
    df = spark.read.parquet(ring_feat_path)
    df1 = spark.read.parquet(ring_out_path)
    df2 = spark.read.parquet(ring_map_path)
    df3 = spark.read.parquet(ring_prod_path)
    
    df1 = df1.join(df, on='id', how='inner')
    
    w = Window.partitionBy("BUREAU_REFERENCE_NUMBER").orderBy(F.desc("created_at"))
    
    df3_latest = df3 \
        .withColumn("rn", F.row_number().over(w)) \
        .filter(F.col("rn") == 1) \
        .drop("rn", "INPUT_VALUE", "OUTPUT_VALUE", "PLATFORM") \
        .withColumn('platform', F.when(F.col('channel') == 'RING_APP', F.lit('RING')).otherwise(F.lit('WEB_AGG')))
    
    df_r = df1.join(df3_latest, df1.id == df3_latest.BUREAU_REFERENCE_NUMBER, "inner")
    df_r1 = df_r.toDF(*[c.lower() for c in df_r.columns])
    
    cols_to_drop = [col for col in df_r1.columns if col.startswith('model')] + \
                   ['module', 'aa_status', 'channel', 'loan_amount']
    cols_to_drop = list(set(cols_to_drop))
    df_r1 = df_r1.drop(*cols_to_drop)
    
    print(" RING data processed successfully")
    return df_r1


def merge_and_transform_data(df_k1, df_r1):
    """
    Merge KISSHT and RING data and apply transformations.
    
    Args:
        df_k1: Processed KISSHT dataframe
        df_r1: Processed RING dataframe
        
    Returns:
        DataFrame: Merged and transformed data
    """
    print(" Merging and Transforming Data")
    
    df_final = df_k1.unionByName(df_r1)
    
    schema = MapType(StringType(), StringType())
    df_final = df_final \
        .withColumn("NEW_BUREAU_VARIABLES", F.from_json("NEW_BUREAU_VARIABLES", schema)) \
        .withColumn("active_cc_count", F.col("NEW_BUREAU_VARIABLES").getItem("active_cc_count")) \
        .withColumn('active_pl_count', F.col("NEW_BUREAU_VARIABLES").getItem('active_pl_count')) \
        .withColumnRenamed('v30_pred_prob', 'output_value') \
        .withColumnRenamed('USER_TYPE_FINAL', 'user_type') \
        .withColumnRenamed('score', 'bureau_score') \
        .withColumnRenamed('instalment_no_months', 'tenure')
    
    print(" Data merged and transformed successfully")
    return df_final


def convert_to_parquet_compatible(df):
    """
    Convert pandas DataFrame to Parquet-compatible types.
    
    Args:
        df (pd.DataFrame): Input dataframe
        
    Returns:
        pd.DataFrame: Converted dataframe with Parquet-compatible types
    """
    df = df.copy()
    
    for col in df.columns:
        if df[col].dtype == 'object':
            sample_val = df[col].dropna().iloc[0] if len(df[col].dropna()) > 0 else None
            
            if sample_val is not None:
                if isinstance(sample_val, (list, dict, tuple, set)):
                    df[col] = df[col].apply(lambda x: json.dumps(x) if pd.notna(x) else None)
                elif isinstance(sample_val, (int, float, np.integer, np.floating)):
                    try:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                    except:
                        df[col] = df[col].astype(str)
                else:
                    df[col] = df[col].astype(str).replace('nan', None)
    
    return df