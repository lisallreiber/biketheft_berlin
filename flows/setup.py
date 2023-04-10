# script to run for the first time only

from pathlib import Path
from prefect import flow, task                      # to create a flow and tasks
from prefect_gcp.cloud_storage import GcsBucket

@task(name="Load GeoData")
def load_geo(geo_path: Path) -> None:
    """Load GeoData"""
    gcs_bucket_block = GcsBucket.load("de-zoomcamp-gcs")

    try:
        print(geo_path)
        gcs_bucket_block.upload_from_folder(
            from_folder=geo_path,
            to_folder=geo_path
        )

    except Exception as e:
        print(f"Could not load data to GCS: {e}")
    return

@task(name="Load Archive Data")
def load_archive(archive_path: Path) -> None:
    """Load Archive Data"""
    gcs_bucket_block = GcsBucket.load("de-zoomcamp-gcs")

    try:
        print(archive_path)
        gcs_bucket_block.upload_from_folder(
            from_folder=archive_path,
            to_folder=archive_path
        )

    except Exception as e:
        print(f"Could not load data to GCS: {e}")
    return

@flow(name="Local to GCS")
def etl_local_to_gcs():
    """Main ETL Flow to load data into BigQuery"""
    # define local geo path
    geo_path = Path("data/seeds/LOR_Planung_Berlin_Geo")

    if geo_path.is_dir():
        load_geo(geo_path)
    else:
        raise FileNotFoundError("File not found")
    
    # define local archive path
    archive_path = Path("data/raw/archive")

    if archive_path.is_dir():
        load_archive(archive_path)
    else:
        raise FileNotFoundError("File not found")

if __name__ == "__main__":
    etl_local_to_gcs()


