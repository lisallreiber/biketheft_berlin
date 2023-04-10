from pathlib import Path                            # standard libary to deal with file paths
from pandas import pandas as pd                     # popular lib to work with data
from prefect import flow, task                      # to create a flow and tasks
from prefect_gcp.cloud_storage import GcsBucket     # to upload to GCS
from prefect_gcp import GcpCredentials              # to authenticate to GCP


@task(name="Extract from GSC", retries=3)
def extract_from_gsc(gcs_path: Path) -> Path:
    """Download trip data from GCS"""
    gcs_bucket_block = GcsBucket.load("de-zoomcamp-gcs")
    gcs_bucket_block.get_directory(
        from_path=gcs_path,
        local_path="."
    )
    """Read data from GCS"""
    
    return Path(f"{gcs_path}")

@task(name="Transform data from GSC",log_prints=True)
def transform(data_path: Path) -> pd.DataFrame:
    df = pd.read_csv(
            data_path,
            encoding='latin-1',
            parse_dates=['ANGELEGT_AM', 'TATZEIT_ANFANG_DATUM', 'TATZEIT_ENDE_DATUM'],
            dayfirst=True)
    print(f"columns: {df.dtypes}")

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

# @task(name="Transform archived data from GSC")
# def transform(path: Path) -> pd.DataFrame:
#     """Transform data from GCS"""
#     df = pd.read_csv(path, encoding='latin-1')

#     return df

@task(name="Write to BigQuery", log_prints=True)
def write_bq(df: pd.DataFrame, table_name: str):
    """Write data to BigQuery"""

    gcp_credentials_block = GcpCredentials.load("de-zoomcamp-project-dtc-de")

    df.to_gbq(
        destination_table=f"berlin_bike_theft.{table_name}",
        project_id="dtc-de-375708",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        if_exists="append"
    )

@flow(name="ETL GCS to BQ")
def etl_gcs_to_bq(toggle_setup: bool):
    """Main ETL Flow to load data into BigQuery"""

    if toggle_setup:
        # configure flow
        archived_reports_path = f"data/raw/archive/2021_2022_Fahrraddiebstahl.csv"
        archived_reports_df = transform(archived_reports_path)
        write_bq(archived_reports_df, "reported_incidents_archived")

        mappings_path = Path("data/mappings/berlin_lor_geo.csv")
        mappings_df = pd.read_csv(mappings_path, sep=";")
        write_bq(mappings_df, "mapping_berlin_lor")

    date = pd.to_datetime('today').date()
    daily_report_path = f"data/raw/daily/{date}_berlin-bike-theft.csv"
    
    gcs_path = extract_from_gsc(daily_report_path)
    reports_df = transform(gcs_path)
    write_bq(reports_df, "reported_incidents_daily")

if __name__ == "__main__":
    etl_gcs_to_bq(toggle_setup = False)


    