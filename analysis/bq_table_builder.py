import os
from google.cloud import bigquery
from dotenv import load_dotenv

load_dotenv()

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BQ_DATASET = os.getenv("BQ_DATASET")
GCS_BUCKET = os.getenv("GCS_BUCKET")


def run_bq_sql():
    client = bigquery.Client(project=PROJECT_ID)

    with open("sql/youtube_video_features.sql", "r") as f:
        video_sql = f.read()    

    with open("sql/gmap_place_features.sql", "r") as f:
        place_sql = f.read()

    video_sql = video_sql.replace("${PROJECT_ID}", PROJECT_ID).replace("${BQ_DATASET}", BQ_DATASET).replace("${GCS_BUCKET}", GCS_BUCKET)
    place_sql = place_sql.replace("${PROJECT_ID}", PROJECT_ID).replace("${BQ_DATASET}", BQ_DATASET).replace("${GCS_BUCKET}", GCS_BUCKET)

    run_query(client, video_sql, "YouTube Video Features")
    run_query(client, place_sql, "Google Maps Place Features")

    print(f"All queries finished. Tables created in {PROJECT_ID}.{BQ_DATASET}")

def run_query(client, sql, name):
    try:
        print(f"Running {name} query...")
        job = client.query(sql)
        job.result()  
        print(f"{name} query completed successfully")
    except Exception as e:
        print(f"ERROR in {name} query")
        print("----- SQL Start -----")
        print(sql[:500])  
        print("----- SQL End -------")
        raise e  


if __name__ == "__main__":
    run_bq_sql()