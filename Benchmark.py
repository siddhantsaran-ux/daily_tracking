import importlib
from benchmark.spark_handler import get_or_create_spark
from benchmark.create_benchmark_table import create_benchmark_table
from benchmark.upload_to_snowflake import upload_benchmark_to_snowflake
from benchmark.view_benchmark import view_benchmark_from_snowflake, fetch_benchmark_from_snowflake


class Benchmark:
    """
    Loads:
        - base_config (Snowflake + infra)
        - model_config based on model_name during create_benchmark()
    """

    def __init__(self, base_config_path="benchmark.base_config"):
        """
        Initialize Benchmark with only infra-level config.
        """
        module = importlib.import_module(base_config_path)
        self.config = getattr(module, "BASE_CONFIG")

        self.spark = None

        print("Benchmark initialized")
        print(" Loaded base infra config")


    def _init_spark(self):
        """Initialize Spark session once."""
        if self.spark is None:
            self.spark = get_or_create_spark()
            print(" Spark session ready")


    def create_benchmark(
        self,
        platform,
        user_type,
        metric_name=None,
        value_name=None,
        model_name=None,
        benchmark_name=None,
        benchmark_start_date=None,
        benchmark_end_date=None,
        benchmark_end_period=None,
        save_intermediate=True,
        data_source="other",
        bench_path=None,
        model_config_path="benchmark.model_config",
        metric_dict=None
    ):
        """
        Create benchmark for (platform, user_type, model_name).

        Args:
            platform (str or list)
            user_type (str or list)
            metric_name (str)
            value_name (str or list)
            metric_dict (dict, optional): If provided, metric_name/value_name are ignored
        """

        # Load model-specific config dictionary
        cfg_module = importlib.import_module(model_config_path)
        MODEL_CONFIG = getattr(cfg_module, "MODEL_CONFIG")

        if model_name not in MODEL_CONFIG:
            raise KeyError(
                f"Model '{model_name}' not found in MODEL_CONFIG. "
                f"Available models: {list(MODEL_CONFIG.keys())}"
            )
        model_cfg = MODEL_CONFIG[model_name]

        print("\n===== Creating Benchmark =====")
        print(f"Model: {model_name}")
        print(f"Platform: {platform}")
        print(f"User Type: {user_type}")

        # --- Normalize platform and user_type to lists ---
        if isinstance(platform, str):
            platforms = [platform]
        else:
            platforms = platform

        if isinstance(user_type, str):
            user_types = [user_type]
        else:
            user_types = user_type

        # --- Normalize value_name only if metric_dict is not passed ---
        if metric_dict is None:
            print(f"Metric: {metric_name}")
            print(f"Value Name: {value_name}")

            if isinstance(value_name, str):
                value_name = [value_name]

            metric_dict = {metric_name: value_name}
        else:
            print("Using provided metric_dict (metric_name and value_name ignored).")

        # Load metadata (cuts, fillna, feature cuts)
        from benchmark.metadata_loader import load_metadata
        meta_dict = load_metadata(model_cfg)
        
        # Expand metric_dict values when "all_features" is used 
        fillna_features = list(meta_dict.get("fillna_dict", {}).keys())

        new_metric_dict = {}
        for m_name, m_val in metric_dict.items():
            if m_val == "all_features":
                # Replace "all_features" with all feature names
                new_metric_dict[m_name] = fillna_features
            else:
                # Keep original value as-is
                new_metric_dict[m_name] = m_val

        metric_dict = new_metric_dict

        
        
        # Start spark
        self._init_spark()
        final_path = bench_path if bench_path else self.config["bench_out"]

        # Create benchmark using model-specific config
        df = create_benchmark_table(
            spark=self.spark,
            config=model_cfg,
            metric_dict=metric_dict,
            meta_dict=meta_dict,
            platforms=platforms,
            user_types=user_types,
            model_name=model_name,
            benchmark_name=benchmark_name,
            benchmark_start_date=benchmark_start_date,
            benchmark_end_date=benchmark_end_date,
            benchmark_end_period=benchmark_end_period,
            save_intermediate=save_intermediate,
            bench_path=final_path
        )

        print("Benchmark created")
        return df


    def upload_benchmark(self, parquet_path, verify_upload=True):
        """Upload benchmark table to Snowflake."""
        print("\n===== Uploading Benchmark to Snowflake =====")
        return upload_benchmark_to_snowflake(
            config=self.config,
            parquet_path=parquet_path,
            verify_upload=verify_upload
        )

    def view_benchmark(self,
    benchmark_name=None,
    platform=None,
    user_type=None,
    metric_name=None,
    model_name=None,
    value_name=None,
    created_at=None,
    limit=None
):
        """View benchmark rows in Snowflake with extended filters."""
    
        return view_benchmark_from_snowflake(
            config=self.config,
            benchmark_name=benchmark_name,
            platform=platform,
            user_type=user_type,
            metric_name=metric_name,
            model_name=model_name,
            value_name=value_name,
            created_at=created_at,
            limit=limit
        )


    def get_benchmark(self, model_name, output_path=None):
        """Download benchmark table from Snowflake."""
        return fetch_benchmark_from_snowflake(
            config=self.config,
            model_name=model_name,
            output_path=output_path
        )