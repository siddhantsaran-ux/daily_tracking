"""
Metadata loading functionality for BenchmarkManager.
"""
import pickle


def load_metadata(config):
    """
    Load metadata dictionaries required for benchmark creation.
    
    Args:
        config (dict): Configuration dictionary with paths
        
    Returns:
        tuple: (metric_dict, meta_dict)
    """
    try:
        # Load pickle files
        score_cuts = pickle.load(open(config['cut_point_path'], 'rb'))
        fillna_dict_raw = pickle.load(open(config['dict_path'], 'rb'))
        fillna_dict = {k.lower(): v for k, v in fillna_dict_raw.items()}
        feature_cuts = pickle.load(open(config['feature_cut_path'], 'rb'))
        feature_type = pickle.load(open(config['feature_type_path'], 'rb'))
        
        # Create metadata dictionary
        meta_dict = {
            'score_cuts': score_cuts,
            'fillna_dict': fillna_dict,
            'feature_cuts': feature_cuts,
            'feature_type': feature_type
        }
        
        print(" Metadata loaded successfully")
        return meta_dict
        
    except Exception as e:
        print(f" Error loading metadata: {e}")
        raise