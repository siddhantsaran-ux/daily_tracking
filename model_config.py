# L1 data Path 
kissht_l1= '/home/manu.chandra/daily_tracking/data_fetch/v30_model_data/kissht/'
ring_l1= '/home/manu.chandra/daily_tracking/data_fetch/v30_model_data/ring/'

# L1 prod path
kissht_l1_prod = '/home/manu.chandra/daily_tracking/data_fetch/v30_prod_data/new_data_kissht_l1_V30'
ring_l1_prod = '/home/manu.chandra/daily_tracking/data_fetch/v30_prod_data/new_data_ring_l1_V30'



## input file path in data creation for benchmark function
# for kissht
kissht_feat_path = kissht_l1 + 'model_v30_feature_data.parquet'
kissht_out_path = kissht_l1 + 'model_v30_output.parquet'
kissht_map_path = kissht_l1 + 'cir_data_mapping_file.parquet'
kissht_prod_path = kissht_l1_prod +  '/*.snappy.parquet'
# for ring
ring_feat_path = ring_l1 + 'model_v30_feature_data.parquet'
ring_out_path = ring_l1 + 'model_v30_output.parquet'
ring_map_path = ring_l1 + 'cir_data_mapping_file.parquet'
ring_prod_path = ring_l1_prod +  '/*.snappy.parquet'

#  path
cut_point_path = '/home/manu.chandra/daily_tracking/data_fetch/meta_data/score_cut_v30.pickle'
dict_path =  '/home/manu.chandra/daily_tracking/data_fetch/meta_data/fillna_dict_v30.pickle'
feature_cut_path = '/home/manu.chandra/daily_tracking/data_fetch/meta_data/feature_edges_v30.pkl'
feature_type_path = '/home/manu.chandra/daily_tracking/data_fetch/meta_data/feature_type_rank_v30.pickle'

MODEL_CONFIG = {
    'GLOBAL-V30':  {
            # Paths
            'kissht_l1': kissht_l1,
            'ring_l1': ring_l1 ,
            'kissht_l1_prod': kissht_l1_prod,
            'ring_l1_prod': ring_l1_prod,
            'cut_point_path': cut_point_path,
            'dict_path': dict_path,
            'feature_cut_path': feature_cut_path,
            'feature_type_path': feature_type_path,
            # Feature and output paths for kissht
            'kissht_feat_path': kissht_feat_path,
            'kissht_out_path': kissht_out_path,
            'kissht_map_path': kissht_map_path,
            'kissht_prod_path': kissht_prod_path,
            # Feature and output paths for ring
            'ring_feat_path': ring_feat_path,
            'ring_out_path': ring_out_path,
            'ring_map_path': ring_map_path,
            'ring_prod_path': ring_prod_path,
        }}