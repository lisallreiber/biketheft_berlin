from pathlib import Path                            # standard libary to deal with file paths
from pandas import pandas as pd                     # popular lib to work with data
from prefect import flow, task                      # to create a flow and tasks
from prefect_gcp.cloud_storage import GcsBucket     # to upload to GCS
import os

@task(name="download data", log_prints=True, retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Download data from a URL into Pandas DataFrame"""
    
    df = pd.read_csv(
        dataset_url,
        encoding='latin-1',
        parse_dates=['ANGELEGT_AM', 'TATZEIT_ANFANG_DATUM', 'TATZEIT_ENDE_DATUM'],
        dayfirst=True
    )
    return df

@task(name="clean data",log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    # combine TATZEIT_DATUM and TATZEIT_STUNDE to create TATZEIT
    df['TATZEIT_ANFANG'] = df['TATZEIT_ANFANG_DATUM'] + pd.to_timedelta(df['TATZEIT_ANFANG_STUNDE'], unit='h')
    df['TATZEIT_ENDE'] = df['TATZEIT_ENDE_DATUM'] + pd.to_timedelta(df['TATZEIT_ENDE_STUNDE'], unit='h')

    # extract hour from TATZEIT_ANFANG and assign it to TATZEIT_ANFANG_STUNDE
    df['TATZEIT_ANFANG_STUNDE'] = df['TATZEIT_ANFANG'].dt.hour
    df['TATZEIT_ENDE_STUNDE'] = df['TATZEIT_ENDE'].dt.hour

    # generate tatzeit dauer
    df['TATZEIT_DAUER'] = (df['TATZEIT_ENDE'] - df['TATZEIT_ANFANG']) / pd.Timedelta(hours=1)

    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df

@task(name="write local",log_prints=True)
def write_local(df: pd.DataFrame, local_path: str) -> Path:
    #"""Write DataFrame to local parquet file"""
    # local_prefix = Path(f"data/pq")
    # local_path = f"{local_prefix}/{file_name}.parquet"
    """Write DataFrame to local csv file"""
    df.to_csv(local_path, index=False)
    return

@task(name="write GCS",log_prints=True)
def write_gcs(path: Path) -> None:
    """Write local csv file to GCS"""
    gcp_bucket = GcsBucket.load("de-zoomcamp-gcs")
    gcp_bucket.upload_from_path(from_path=path)
    return

@flow(name="Main ETL", log_prints=True)
def etl_web_to_gcs() -> None:
    """Main ETL Function"""
    # configure flow
    dataset_url = "https://www.internetwache-polizei-berlin.de/vdb/Fahrraddiebstahl.csv"
    # assign today's date to variable today via os.system
    today = pd.to_datetime('today').date()
    file_name = f"{today}_berlin-bike-theft"
    
    local_prefix = Path(f"data/raw")
    local_path = f"{local_prefix}/{file_name}.csv"
        
    # Get the file with the most recent date
    available_dates = [file.split('_')[0] for file in os.listdir(local_prefix)]
    most_recent_date = max(available_dates)
    
    print(f"data is available locally for data up to {most_recent_date}")

    if os.path.exists(local_path):
        print(f"... skipping download, processed file exists {local_path}")
        df_clean = pd.read_csv(local_path)
    else :
        print(f"... downloading {dataset_url} for date {today}")
        # create local path if it does not exist
        local_path.parent.mkdir(parents=True, exist_ok=True)
        df_raw = fetch(dataset_url)
        df_clean = clean(df_raw)    
        write_local(df_clean, local_path)
        print(f"... to location: local {local_path}")

    write_gcs(local_path)
    print(f"... to location: GCS {local_path}")

if __name__ == "__main__":
    etl_web_to_gcs()