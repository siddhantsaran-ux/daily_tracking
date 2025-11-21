from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import MapType, StringType, DoubleType,TimestampType, DateType
import pickle
import json
import openpyxl
import pandas as pd
from itertools import chain
from datetime import datetime, timedelta
import re
import os
import uuid


def create_benchmark(df, 
                     platform,
                     user_type,
                     model_name,
                     metric_dict,
                     meta_dict,
                     benchmark_name,
                     benchmark_start_time = None,
                     benchmark_end_time = None,
                     tracking_type='DAILY',
                     is_active=True,
                     start_date=None,
                     end_date=None):
    """
    Creates benchmark DataFrame for given metrics and parameters.
    """

    benchmark_created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    # benchmark_id = str(uuid.uuid4())
    model_name = model_name.upper()


    fillna_dict = meta_dict.get('fillna_dict', {})
    feature_cuts = meta_dict.get('feature_cuts', {})
    score_cuts = meta_dict.get('score_cuts', {})

    df_filtered = df.filter(
        (F.col('platform') == platform) &
        (F.col('user_type') == user_type)
    )
    if df_filtered.count() == 0:
        print('no count coming')
        return 
    all_results = []

    for metric_name, value_names in metric_dict.items():
        metric = {}
        metric['metric_name'] = metric_name
        metric['value_name'] = value_names
        results = create_metric(df_filtered,metric,meta_dict)

        for res in results:
            res.update({
                'benchmark_created_at': benchmark_created_at,
                'benchmark_name': benchmark_name,
                # 'benchmark_id': benchmark_id,
                'benchmark_start_time': benchmark_start_time,
                'benchmark_end_time': benchmark_end_time,
                'tracking_type': tracking_type,
                'model_name': model_name,
                'platform': platform,
                'user_type': user_type,
                'is_active': is_active,
                'start_date': start_date,
                'end_date': end_date,
                'meta_data': str(meta_dict)  # store meta dict as string (JSON-like)
            })
            all_results.append(res)

    if not all_results:
        return None

# Sanitize & flatten all result dicts
    for r in all_results:
        for k, v in r.items():
            # Convert complex types to string
            if isinstance(v, (uuid.UUID, datetime)):
                r[k] = str(v)
            elif isinstance(v, (list, dict)):
                r[k] = json.dumps(v)

    # Convert to Pandas DataFrame
    pd_df = pd.DataFrame(all_results)

    # Reorder columns for consistency
    ordered_cols = [
        'benchmark_created_at', 'benchmark_name', 'benchmark_id',
        'benchmark_start_time', 'benchmark_end_time', 'tracking_type',
        'model_name', 'platform', 'user_type', 'metric_name', 'value_name',
        'cutpoints', 'bin_counts', 'categories', 'category_count',
        'total_count', 'is_active', 'start_date', 'end_date', 'meta_data'
    ]
    pd_df = pd_df[[c for c in ordered_cols if c in pd_df.columns]]

    return pd_df


#segment_distribution
def segment_dist(df, metric_name, value_names):
    results = []

    for val in value_names:
        val_lower = val.lower()

        if val_lower == 'employment_type':
            df_seg = df.withColumn(
                "segment_band",
                F.when((F.col(val).isNull()) | (F.col(val) == " ") | (F.col(val) == "none"), "NA")
                 .otherwise(F.col(val))
            )

        elif val_lower == 'bureau_score':
            df_seg = df.withColumn(val, F.col(val).cast("double")).withColumn(
                "segment_band",
                F.when(F.col(val) >= 750, "750+")
                 .when(F.col(val) < 720, "<720")
                 .when((F.col(val) >= 720) & (F.col(val) < 750), "[720,750)")
                 .otherwise("NA")
            )

        elif val_lower =='tenure':
            df_seg = df.withColumn(val, F.col(val).cast("double")).withColumn(
                "segment_band",
                F.when(F.col(val) < 6, "<6")
                 .when((F.col(val) >= 6) & (F.col(val) <= 8), "6-8")
                 .when(F.col(val) > 8, ">8")
                 .otherwise("NA")
            )

        elif val_lower == 'bureau_vintage':
            df_seg = df.withColumn(val, F.col(val).cast("double")).withColumn(
                "segment_band",
                F.when(F.col(val) < 12, "<12")
                 .when((F.col(val) >= 12) & (F.col(val) <= 60), "12-60")
                 .when(F.col(val) > 60, ">60")
                 .otherwise("NA")
            )

        elif val_lower == 'no_of_cc_loan_entries':
            df_seg = df.withColumn(val, F.col(val).cast("double")).withColumn(
                "segment_band",
                F.when(F.col(val) == 1, "1CC")
                 .when(F.col(val) > 1, "1+CC")
                 .when(F.col(val) == 0, "No CC")
                 .otherwise("NA")
            )

        elif val_lower == 'active_cc_count':
            df_seg = df.withColumn(val, F.col(val).cast("double")).withColumn(
                "segment_band",
                F.when(F.col(val) < 1, "<1")
                 .when(F.col(val) >= 1, "1 or 1+")
                 .otherwise("NA")
            )

        elif val_lower == 'active_pl_count':
            df_seg = df.withColumn(val, F.col(val).cast("double")).withColumn(
                "segment_band",
                F.when(F.col(val) < 1, "<1")
                 .when((F.col(val) >= 1) & (F.col(val) <= 5), "1-5")
                 .when(F.col(val) > 5, ">5")
                 .otherwise("NA")
            )

        elif val_lower == 'unsecured_enquiries_in_last_1_mon':
            df_seg = df.withColumn(val, F.col(val).cast("double")).withColumn(
                "segment_band",
                F.when(F.col(val) < 1, "<1")
                 .when((F.col(val) >= 1) & (F.col(val) <= 10), "1-10")
                 .when(F.col(val) > 10, "10+")
                 .otherwise("NA")
            )

        elif val_lower == 'months_since_last_90_dpd':
            df_seg = df.withColumn(val, F.col(val).cast("double")).withColumn(
                "segment_band",
                F.when(F.col(val) < 1, "<1")
                 .when((F.col(val) >= 1) & (F.col(val) <= 12), "1-12")
                 .when(F.col(val) > 12, "12+")
                 .otherwise("NA")
            )

        else:
            continue

        df_count = (
            df_seg.groupBy("segment_band")
                  .agg(F.count("*").alias("count"))
                  .orderBy("segment_band")
        )

        categories = [r["segment_band"] for r in df_count.collect()]
        counts = [r["count"] for r in df_count.collect()]
        output_dict = {
            'benchmark_id': str(uuid.uuid4()),
            "metric_name": metric_name.upper(),
            'cutpoints': None,
            'bin_counts': None,
            "value_name": val,
            "categories": categories,
            "category_count": counts,
            "total_count": sum(counts)
        }

        results.append(output_dict)

    return results

#invalid score
#invalid tagging
def invalid_metric(df, metric_name):
    metric_name = metric_name.lower()
    results = []

    if metric_name == 'invalid_score':
        bands = ["<0", "=0", ">=1", "0-1", "NULL"]

        df_bands = (
            df.withColumn("output_value", F.col("output_value").cast("double"))
              .withColumn(
                  "band_name",
                  F.when(F.col("output_value").isNull(), F.lit("NULL"))
                   .when(F.col("output_value") < 0, F.lit("<0"))
                   .when(F.col("output_value") == 0, F.lit("=0"))
                   .when(F.col("output_value") >= 1, F.lit(">=1"))
                   .otherwise(F.lit("0-1"))
              )
        )

        df_counts = (
            df_bands.groupBy("band_name")
                    .agg(F.count("*").alias("count"))
        )

        df_complete = (
            df_counts.unionByName(
                df_counts.sparkSession.createDataFrame(
                    [(b, 0) for b in bands if b not in [r["band_name"] for r in df_counts.collect()]],
                    df_counts.schema
                ),
                allowMissingColumns=True
            )
            .orderBy(F.when(F.col("band_name") == "<0", 1)
                        .when(F.col("band_name") == "=0", 2)
                        .when(F.col("band_name") == "0-1", 3)
                        .when(F.col("band_name") == ">=1", 4)
                        .when(F.col("band_name") == "NULL", 5))
        )

        band_names = [r["band_name"] for r in df_complete.collect()]
        band_counts = [r["count"] for r in df_complete.collect()]

        output_dict = {
            'benchmark_id': str(uuid.uuid4()),
            'metric_name': 'INVALID_SCORE',
            'value_name': 'total_population',
            'cutpoints': None,
            'bin_counts': None,
            'total_count': sum(band_counts),
            'categories': band_names,
            'category_count': band_counts
        }

        results.append(output_dict)
        return results

    else:
        return []
    

# feature_dist(df_kr1,'FEATURE_RANGE_DRIFT' ,feat_list,feature_cuts)
def generate_case_expr(feature, edges):
    """
    Generate a CASE expression that bins `value` into numeric bands (1,2,3,...)
    using quantile-based edges with universal lower/upper limits (-inf, +inf).
    """
    if not edges or len(edges) < 2:
        return "CASE WHEN value IS NOT NULL THEN 1 ELSE NULL END AS feature_band"

    # Remove duplicates and sort
    edges = sorted(set(float(e) for e in edges if e is not None))
    
    # Skip first and last (use -inf and +inf instead)
    inner_edges = edges[1:-1] if len(edges) > 2 else []

    conds = []
    # Bin counter
    bin_id = 1

    # 1️⃣ First bin: (-inf, first_edge]
    first_edge = inner_edges[0] if inner_edges else edges[-1]
    conds.append(f"WHEN value <= {first_edge} THEN {bin_id}")
    bin_id += 1

    # 2️⃣ Middle bins
    for i in range(len(inner_edges) - 1):
        low = inner_edges[i]
        high = inner_edges[i + 1]
        conds.append(f"WHEN value > {low} AND value <= {high} THEN {bin_id}")
        bin_id += 1

    # 3️⃣ Last bin: (last_edge, +inf)
    last_edge = inner_edges[-1] if inner_edges else edges[0]
    conds.append(f"WHEN value > {last_edge} THEN {bin_id}")

    expr = f"CASE {' '.join(conds)} ELSE NULL END AS feature_band"
    return expr


#CSI
#feature_range_drift
def feature_dist(df, metric_name, value_name, feature_cuts,fillna_dict ,id_cols=None):
    # normalize inputs
    if isinstance(value_name, list):
        value_name = [v.lower() for v in value_name]
    else:
        value_name = [value_name.lower()]
    metric_name = metric_name.lower()

    # feature_cuts contains {feature_name: [cutpoints]}
    feature_cuts = {k.lower(): v for k, v in feature_cuts.items()}
    l2 = list(feature_cuts.keys())
    n = len(l2)

    if id_cols is None:
        id_cols = []

    # select only relevant columns
    df = df.select(*id_cols, *l2)
    df = df.fillna(fillna_dict)
    # stack the columns into key-value pairs
    stack_expr = f"stack({n}, " + ", ".join([f"'{col}', `{col}`" for col in l2]) + ") as (feature, value)"
    df1 = df.select(*[F.col(c) for c in id_cols], F.expr(stack_expr))

    feat_dict = {}

    # loop through each feature in feature_cuts
    for feat, edges in feature_cuts.items():
        
        if feat not in value_name:
            continue  # only process requested features

        # generate case expression for banding
        case_expr = generate_case_expr(feat, edges)

        # apply feature-specific bands
        df_feat = df1.filter(F.col("feature") == feat).selectExpr("*", case_expr)

        # aggregate by band
        df_agg = (
            df_feat.groupBy("feature_band")
                   .agg(F.count("*").alias("feature_count"))
                   .orderBy("feature_band")
        )
        
        # collect results
        bands = edges
        counts = [r["feature_count"] for r in df_agg.collect()]

        # convert to percentage if metric is CSI
        total = sum(counts)
        
        if metric_name == "csi":
            if total > 0:
                counts = [round((c / total) * 100, 4) for c in counts]
            else:
                counts = [0 for _ in counts]
            total = sum(counts)
        # build dictionary
        feat_dict[feat] = [bands, counts,total]

    return feat_dict

#Null rate
#distinct rate
def feature_trend(df, metric_name, value_name, fillna_dict, id_cols=None):
    if isinstance(value_name, list):
        value_name = [v.lower() for v in value_name]
    else:
        value_name = [value_name.lower()]
    metric_name = metric_name.lower()
    fillna_dict = {k.lower(): v for k, v in fillna_dict.items()}  # lowercase all keys
    l2 = list(fillna_dict.keys())
    n = len(l2)

    if id_cols is None:
        id_cols = []  # fallback if not provided

    # Stack the columns into key-value pairs
    stack_expr = f"stack({n}, " + ", ".join([f"'{col}', `{col}`" for col in l2]) + ") as (feature, value)"
    df1 = df.select(*[F.col(c) for c in id_cols], F.expr(stack_expr))

    final_dict = {}

    if metric_name == 'null_rate':
        # Create a map of feature -> fillna reference values
        mapping_expr = F.create_map([F.lit(x) for x in sum([[k, v] for k, v in fillna_dict.items()], [])])

        # Aggregate by feature
        df2 = df1.groupBy("feature")\
               .agg(
                   F.sum(
                       F.when(
                           (F.col("value") == mapping_expr[F.col("feature")]) | F.col("value").isNull(),
                           1
                       ).otherwise(0)
                   ).alias("null_count"),
                   F.count("feature").alias("total_count")
               )
        

        # Filter to include only features present in value_name
        df2 = df2.filter(F.col("feature").isin(value_name))

        # Collect results and build dictionary
        rows = df2.collect()
        final_dict = {r["feature"]: [r["null_count"], r["total_count"]] for r in rows}

        return final_dict
    if metric_name == 'distinct_rate':
        df2 = df1.groupBy("feature")\
               .agg(
                   F.count("feature").alias("total_count"),
                   F.countDistinct("value").alias('distinct_count')
               )
        

        # Filter to include only features present in value_name
        df2 = df2.filter(F.col("feature").isin(value_name))

        # Collect results and build dictionary
        rows = df2.collect()
        final_dict = {r["feature"]: [r["distinct_count"], r["total_count"]] for r in rows}

    return final_dict


#approval trends
def trend_metric(df, value_name_list, output_dict):
    # Step 1: Define expressions
    expr2 = [
        "*",
        "CASE WHEN status = 'FINAL_APPROVED' THEN 1 ELSE 0 END AS final_approved",
        "CASE WHEN status = 'COND_APPROVED' THEN 1 ELSE 0 END AS cond_approved",
        "CASE WHEN status = 'DENIED_1' THEN 1 ELSE 0 END AS denied_1",
        "CASE WHEN status = 'DENIED_2' THEN 1 ELSE 0 END AS denied_2",
    ]
    
    # Step 2: Apply transformations
    df2 = df.selectExpr(expr2)
    df3 = df2.groupBy("model_band").agg(
        F.sum("final_approved").alias("fa_count"),
        F.sum("cond_approved").alias("ca_count"),
        F.sum("denied_1").alias("d1_count"),
        F.sum("denied_2").alias("d2_count"),
    )

    # Step 3: Window for totals (global)
    w = Window.partitionBy()

    df4 = (
        df3.withColumn('fa_total', F.sum('fa_count').over(w))
           .withColumn('ca_total', F.sum('ca_count').over(w))
           .withColumn('d1_total', F.sum('d1_count').over(w))
           .withColumn('d2_total', F.sum('d2_count').over(w))
           .withColumn(
                "ca_fa_ratio",
                F.when(F.col("fa_count") == 0, 0)
                 .otherwise(F.col("ca_count") / F.col("fa_count"))
            )
           .withColumn(
                "d1_fa_ratio",
                F.when(F.col("fa_count") == 0, 0)
                 .otherwise(F.col("d1_count") / F.col("fa_count"))
            )
           .orderBy('model_band')
    ).withColumn('ca_fa_total',F.sum('ca_fa_ratio').over(w))\
    .withColumn('d1_fa_total',F.sum('d1_fa_ratio').over(w))

    # Step 4: Build final dictionary
    final_dict = {}

    # Loop through all value names in the list
    for val_name in value_name_list:
        val_lower = val_name.lower()

        # Create a copy of output_dict to avoid mutating input
        temp_dict = output_dict.copy()

        if val_lower == "fa":
            temp_dict["cutpoints"] = None
            temp_dict["bin_counts"] = None
            temp_dict["total_count"] = df4.select('fa_total').first().fa_total
            temp_dict["categories"] = [r.model_band for r in df4.select('model_band').collect()]
            temp_dict["category_count"] = [r.fa_count for r in df4.select('fa_count').collect()]

        elif val_lower == "ca":
            temp_dict["cutpoints"] = None
            temp_dict["bin_counts"] = None
            temp_dict["total_count"] = df4.select('ca_total').first().ca_total
            temp_dict["categories"] = [r.model_band for r in df4.select('model_band').collect()]
            temp_dict["category_count"] = [r.ca_count for r in df4.select('ca_count').collect()]

        elif val_lower == "d1":
            temp_dict["cutpoints"] = None
            temp_dict["bin_counts"] = None
            temp_dict["total_count"] = df4.select('d1_total').first().d1_total
            temp_dict["categories"] = [r.model_band for r in df4.select('model_band').collect()]
            temp_dict["category_count"] = [r.d1_count for r in df4.select('d1_count').collect()]

        elif val_lower == "d2":
            temp_dict["cutpoints"] = None
            temp_dict["bin_counts"] = None
            temp_dict["total_count"] = df4.select('d2_total').first().d2_total
            temp_dict["categories"] = [r.model_band for r in df4.select('model_band').collect()]
            temp_dict["category_count"] = [r.d2_count for r in df4.select('d2_count').collect()]
        
        elif val_lower == 'ca/fa':
            temp_dict["cutpoints"] = None
            temp_dict["bin_counts"] = None
            temp_dict["total_count"] = df4.select('ca_fa_total').first().ca_fa_total
            temp_dict["categories"] = [r.model_band for r in df4.select('model_band').collect()]
            temp_dict["category_count"] = [r.ca_fa_ratio for r in df4.select('ca_fa_ratio').collect()]
            
        elif val_lower == "d1/fa":
            temp_dict["cutpoints"] = None
            temp_dict["bin_counts"] = None
            temp_dict["total_count"] = df4.select('d1_fa_total').first().d1_fa_total
            temp_dict["categories"] = [r.model_band for r in df4.select('model_band').collect()]
            temp_dict["category_count"] = [r.d1_fa_ratio for r in df4.select('d1_fa_ratio').collect()]



        # Add to final dictionary
        final_dict[val_name] = temp_dict

    return final_dict

#Main funciton

import uuid
from pyspark.sql import functions as F, Window

def create_metric(df, metric_dict, meta_dict):
    metric_name = metric_dict['metric_name']
    value_names = metric_dict['value_name']   # can be single or list
    score_cuts = meta_dict['score_cuts']
    fillna_dict = meta_dict['fillna_dict']
    fillna_dict = {k.lower(): v for k, v in fillna_dict.items()}
    if isinstance(value_names, str):
        value_names = [value_names]

    results = []

    # Build model bands
    model_bands = {}
    for i in range(len(score_cuts) - 1):
        model_bands[i + 1] = [score_cuts[i], score_cuts[i + 1]]

    # CASE WHEN logic
    case_expr = "CASE "
    for band, (lower, upper) in model_bands.items():
        case_expr += f"WHEN output_value >= {lower} AND output_value < {upper} THEN {band} "
    case_expr += "ELSE NULL END as model_band"
    expr1 = ["*"] + [case_expr]

    # Process only for relevant metrics
    if metric_name == 'INVALID_SCORE':
        return invalid_metric(df, metric_name)

    if metric_name == 'SEGMENT_DISTRIBUTION':
        return segment_dist(df, metric_name, value_names)

    if metric_name in ['NULL_RATE', 'DISTINCT_RATE']:
        f_dict = feature_trend(df, metric_name, value_names, fillna_dict)
        for k, v in f_dict.items():
            output_dict = {
                'benchmark_id': str(uuid.uuid4()),        # ✅ Added
                'metric_name': metric_name,
                'value_name': k,
                'cutpoints': None,
                'bin_counts': v[0],
                'total_count': v[1],
                'categories': None,
                'category_count': None
            }
            results.append(output_dict)
        return results

    if metric_name == 'CSI':
        f_dict = feature_dist(df, metric_name, value_names, feature_cuts=meta_dict['feature_cuts'],fillna_dict =fillna_dict)
        for k, v in f_dict.items():
            output_dict = {
                'benchmark_id': str(uuid.uuid4()),        # ✅ Added
                'metric_name': metric_name,
                'value_name': k,
                'cutpoints': v[0],
                'bin_counts': v[1],
                'total_count': v[2],
                'categories': None,
                'category_count': None
            }
            results.append(output_dict)
        return results

    if metric_name == 'FEATURE_RANGE_DRIFT':
        f_dict = feature_dist(df, metric_name, value_names, feature_cuts=meta_dict['feature_cuts'],fillna_dict = fillna_dict)
        for k, v in f_dict.items():
            output_dict = {
                'benchmark_id': str(uuid.uuid4()),        # ✅ Added
                'metric_name': metric_name,
                'value_name': k,
                'cutpoints': None,
                'bin_counts': None,
                'total_count': v[2],
                'categories': v[0],
                'category_count': v[1]
            }
            results.append(output_dict)
        return results

    if metric_name in ['PSI', 'DRIFT_IN_MB']:
        df1 = df.selectExpr(expr1)

        # Process trend metrics
        trend_values = [v for v in value_names if 'total_population' not in v.lower()]
        if len(trend_values) > 0:
            trend_result = trend_metric(df1, trend_values, {'metric_name': metric_name})
            for k, v in trend_result.items():
                v['benchmark_id'] = str(uuid.uuid4())     # ✅ Added
                v['value_name'] = k
                results.append(v)

        # Aggregation for population or score metrics
        df2 = df1.groupBy('model_band').agg(F.count('id').alias('count'))
        window = Window.partitionBy()
        df3 = (
            df2.withColumn('total', F.sum('count').over(window))
               .withColumn("pct", F.round((F.col("count") / F.col("total")) * 100, 3))
               .orderBy('model_band')
        )

        if 'score' in value_names:
            output_dict = {
                'benchmark_id': str(uuid.uuid4()),        # ✅ Added
                'metric_name': metric_name,
                'value_name': 'score',
                'cutpoints': score_cuts,
                'bin_counts': [r.pct for r in df3.select('pct').collect()],
                'total_count': df3.select('total').first().total,
                'categories': None,
                'category_count': None
            }
            results.append(output_dict)

        if 'total_population' in value_names:
            output_dict = {
                'benchmark_id': str(uuid.uuid4()),        # ✅ Added
                'metric_name': metric_name,
                'value_name': 'total_population',
                'cutpoints': None,
                'bin_counts': None,
                'total_count': df3.select('total').first().total,
                'categories': [r.model_band for r in df3.select('model_band').collect()],
                'category_count': [r["count"] for r in df3.select('count').collect()]
            }
            results.append(output_dict)

    return results
