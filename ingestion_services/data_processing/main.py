import os
import pandas as pd
import boto3
from io import BytesIO
import time
import json
import numpy as np
from datetime import datetime


def get_env_variables():
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")

    raw_bucket = os.getenv("MINIO_RAW_BUCKET", "raw")
    processing_bucket = os.getenv("MINIO_PROCESSING_BUCKET", "processing")
    loaded_bucket = os.getenv("MINIO_LOADED_BUCKET", "loaded")

    return (
        minio_endpoint,
        access_key,
        secret_key,
        raw_bucket,
        processing_bucket,
        loaded_bucket,
    )


def init_s3_client(endpoint, access_key, secret_key):
    while True:
        try:
            s3_client = boto3.client(
                "s3",
                endpoint_url=endpoint,
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
            )
            s3_client.list_buckets()
            return s3_client
        except Exception as e:
            print(
                f"Erro ao conectar ao MinIO: {e}. Tentando novamente em 5 segundos..."
            )
            time.sleep(5)


def get_latest_file_from_minio(s3_client, bucket, prefix, file_extension):
    try:
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        if "Contents" not in response:
            return None, None

        all_files = [
            obj
            for obj in response.get("Contents", [])
            if obj["Key"].endswith(file_extension)
        ]
        if not all_files:
            return None, None

        latest_file = max(all_files, key=lambda x: x["LastModified"])
        obj = s3_client.get_object(Bucket=bucket, Key=latest_file["Key"])
        return obj["Body"].read(), latest_file["Key"]
    except Exception as e:
        print(f"Erro ao buscar arquivo do MinIO: {e}")
        return None, None


def save_df_to_minio(s3_client, df, bucket, key):
    try:
        out_buffer = BytesIO()
        df.to_parquet(out_buffer, index=False)
        out_buffer.seek(0)
        s3_client.put_object(Bucket=bucket, Key=key, Body=out_buffer.read())
    except Exception as e:
        print(f"Erro ao salvar DataFrame no MinIO: {e}")


def process_data(
    s3_client, df_historical, df_brapi, raw_bucket, processing_bucket, loaded_bucket
):
    processing_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    df_cleaned = df_historical.copy()
    df_cleaned["symbol"] = df_cleaned["CODNEG"].str.strip() + ".SA"
    df_cleaned = df_cleaned.rename(
        columns={
            "PREABE": "Open",
            "PREULT": "Close",
            "PREMAX": "High",
            "PREMIN": "Low",
            "QUATOT": "Volume",
            "DATPRE": "Date",
        }
    )
    df_cleaned["Date"] = pd.to_datetime(df_cleaned["Date"])
    df_cleaned = df_cleaned[
        ["Date", "symbol", "Open", "High", "Low", "Close", "Volume"]
    ]
    df_cleaned.dropna(subset=df_cleaned.columns, inplace=True)
    save_df_to_minio(
        s3_client,
        df_cleaned,
        processing_bucket,
        f"processing/{processing_timestamp}/step1_cleaned.parquet",
    )

    if not df_brapi.empty:
        df_enriched = pd.merge(df_cleaned, df_brapi, on="symbol", how="left")
    else:
        df_enriched = df_cleaned
    save_df_to_minio(
        s3_client,
        df_enriched,
        processing_bucket,
        f"processing/{processing_timestamp}/step2_enriched.parquet",
    )

    df_metrics = df_enriched.copy()
    df_metrics.sort_values(by=["symbol", "Date"], inplace=True)
    df_metrics["Daily_Price_Change"] = df_metrics["Close"] - df_metrics["Open"]
    df_metrics["Daily_Return"] = (
        df_metrics["Close"] - df_metrics["Open"]
    ) / df_metrics["Open"].replace(0, np.nan)
    df_metrics["Price_Volatility"] = (
        df_metrics["High"] - df_metrics["Low"]
    ) / df_metrics["Close"].replace(0, np.nan)
    if "marketCap" in df_metrics.columns:
        financial_volume = df_metrics["Volume"] * df_metrics["Close"]
        df_metrics["Turnover_Ratio"] = financial_volume / df_metrics[
            "marketCap"
        ].replace(0, np.nan)
    save_df_to_minio(
        s3_client,
        df_metrics,
        processing_bucket,
        f"processing/{processing_timestamp}/step3_new_metrics.parquet",
    )

    df_final = df_metrics.copy()
    df_final["SMA_5_Days"] = df_final.groupby("symbol")["Close"].transform(
        lambda x: x.rolling(window=5, min_periods=1).mean()
    )
    df_final["SMA_20_Days"] = df_final.groupby("symbol")["Close"].transform(
        lambda x: x.rolling(window=20, min_periods=1).mean()
    )

    save_df_to_minio(
        s3_client, df_final, loaded_bucket, f"b3_final_processed_{processing_timestamp}.parquet"
    )


def main():
    (
        minio_endpoint,
        access_key,
        secret_key,
        raw_bucket,
        processing_bucket,
        loaded_bucket,
    ) = get_env_variables()
    s3_client = init_s3_client(minio_endpoint, access_key, secret_key)

    parquet_content, _ = get_latest_file_from_minio(
        s3_client, raw_bucket, "kaggle", ".parquet"
    )
    if parquet_content is None:
        print(
            "Erro crítico: Não foi possível carregar os dados históricos da B3. O serviço será encerrado."
        )
        return
    df_historical_base = pd.read_parquet(BytesIO(parquet_content))

    last_processed_brapi_key = None

    while True:
        json_content, latest_brapi_key = get_latest_file_from_minio(
            s3_client, raw_bucket, "brapi", ".json"
        )

        if latest_brapi_key and latest_brapi_key != last_processed_brapi_key:
            last_processed_brapi_key = latest_brapi_key

            brapi_records = [
                json.loads(line)
                for line in json_content.decode("utf-8").strip().split("\n")
            ]
            df_brapi_new = pd.DataFrame(brapi_records)
            df_brapi_new["symbol"] = df_brapi_new["symbol"].str.strip() + ".SA"

            process_data(
                s3_client,
                df_historical_base,
                df_brapi_new,
                raw_bucket,
                processing_bucket,
                loaded_bucket,
            )
        else:
            print("Nenhum dado novo da Brapi encontrado. Aguardando...")

        time.sleep(40)


if __name__ == "__main__":
    main()
