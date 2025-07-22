import os
import shutil
import pandas as pd

def get_env_variables():
    source_path = os.getenv('SOURCE_FILE_PATH', '/app/data/COTAHIST_A2009_to_A2020_P.csv')
    raw_zone_path = os.getenv('RAW_ZONE_PATH', '/data_lake/raw/b3_historical_data')
    return source_path, raw_zone_path

def transform_csv_to_parquet(csv_file_path):
    if not os.path.exists(csv_file_path):
        print(f"Arquivo de origem não encontrado em '{csv_file_path}'")
        return None
    
    try:
        df = pd.read_csv(csv_file_path, encoding='latin-1', sep=';', header=0)

        temp_dir = "/app/tmp"
        os.makedirs(temp_dir, exist_ok=True)
        
        base_name = os.path.splitext(os.path.basename(csv_file_path))[0]
        parquet_path = os.path.join(temp_dir, f"{base_name}.parquet")

        df.to_parquet(parquet_path, index=False)
        return parquet_path
        
    except Exception as e:
        print(f"Ocorreu um erro durante a transformação para Parquet: {e}")
        return None

def ingest_batch_data(source_file, dest_folder):
    if not source_file or not os.path.exists(source_file):
        print(f"Arquivo de origem não fornecido ou não encontrado em '{source_file}'")
        return 
    
    try:
        os.makedirs(dest_folder, exist_ok=True)
        shutil.copy(source_file, dest_folder)
    
    except Exception as e:
        print(f"Ocorreu um erro durante a ingestão: {e}")

def main():
    source_csv, destination_dl = get_env_variables()
    
    parquet_file = transform_csv_to_parquet(source_csv)

    if parquet_file:
        ingest_batch_data(parquet_file, destination_dl)

if __name__ == '__main__':
    main()
