import os
import shutil

def get_env_variables():
    source_path = os.getenv('SOURCE_FILE_PATH', '/app/data/COTAHIST_A2009_to_A2020_P.csv')

    raw_zone_path = os.getenv('RAW_ZONE_PATH', '/data_lake/raw/b3_historical_data')

    return source_path, raw_zone_path

def ingest_batch_data(source_file, dest_folder):
    if not os.path.exists(source_file):
        print(f"Erro: Arquivo de origem não encontrado em '{source_file}'")
        return 
    
    try:
        os.makedirs(dest_folder,exist_ok=True)

        shutil.copy(source_file,dest_folder)
    
    except Exception as e:
        print(f"Ocorreu um erro durante a ingestão: {e}")

def main():
    source, destination = get_env_variables()
    ingest_batch_data(source, destination)

if __name__ == '__main__':
    main()