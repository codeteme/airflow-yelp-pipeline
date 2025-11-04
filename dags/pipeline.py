from __future__ import annotations
import csv
import json
import os
import shutil
import matplotlib.pyplot as plt

from psycopg2 import DatabaseError
from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

OUTPUT_DIR = "/opt/airflow/data/intermediate"
TARGET_TABLE = "yelp_merged"
SCHEMA_NAME = "week10_assignment"

default_args = {
    "owner": "temesgen",
    "retries": 1,
    "retry_delay": timedelta(minutes=2)
}

with DAG(
    dag_id="yelp_pipeline",
    start_date=datetime(2025, 11, 1),
    schedule="@once",  # or "0 22 * * *"
    catchup=False,
) as dag:

    # ---------- Fetch Tasks (parallel) ----------
    @task()
    def fetch_business() -> str:
        input_file = "/opt/airflow/data/raw/yelp_academic_dataset_business.json"
        output_file = os.path.join(OUTPUT_DIR, "business_sample.csv")
        os.makedirs(os.path.dirname(output_file), exist_ok=True)

        with open(input_file, "r", encoding="utf-8") as json_file, \
             open(output_file, "w", newline="", encoding="utf-8") as csv_file:
            sample_size = 5000
            writer = None
            for i, line in enumerate(json_file):
                record = json.loads(line.strip())
                if writer is None:
                    writer = csv.DictWriter(csv_file, fieldnames=list(record.keys()))
                    writer.writeheader()
                writer.writerow(record)
                if i >= sample_size:
                    break
        print(f"Wrote {i} business rows → {output_file}")
        return output_file

    @task()
    def fetch_review() -> str:
        input_file = "/opt/airflow/data/raw/yelp_academic_dataset_review.json"
        output_file = os.path.join(OUTPUT_DIR, "review_sample.csv")
        os.makedirs(os.path.dirname(output_file), exist_ok=True)

        with open(input_file, "r", encoding="utf-8") as json_file, \
             open(output_file, "w", newline="", encoding="utf-8") as csv_file:
            sample_size = 5000
            writer = None
            for i, line in enumerate(json_file):
                record = json.loads(line.strip())
                if writer is None:
                    writer = csv.DictWriter(csv_file, fieldnames=list(record.keys()))
                    writer.writeheader()
                writer.writerow(record)
                if i >= sample_size:
                    break
        print(f"Wrote {i} review rows → {output_file}")
        return output_file

    # ---------- Merge ----------
    @task()
    def merge_csvs(business_path: str, review_path: str) -> str:
        merged_path = os.path.join(OUTPUT_DIR, "merged_yelp.csv")

        with open(business_path, newline="", encoding="utf-8") as f1, \
             open(review_path, newline="", encoding="utf-8") as f2, \
             open(merged_path, "w", newline="", encoding="utf-8") as fout:
            business_reader = {row["business_id"]: row for row in csv.DictReader(f1)}
            review_reader = csv.DictReader(f2)
            fieldnames = ["business_id", "name", "city", "review_stars", "review_text"]
            writer = csv.DictWriter(fout, fieldnames=fieldnames)
            writer.writeheader()

            count = 0
            for review in review_reader:
                bid = review["business_id"]
                if bid in business_reader:
                    b = business_reader[bid]
                    writer.writerow({
                        "business_id": bid,
                        "name": b.get("name"),
                        "city": b.get("city"),
                        "review_stars": review.get("stars"),
                        "review_text": review.get("text", "")[:100],
                    })
                    count += 1
                    if count >= 5000:
                        break
        print(f"Merged {count} rows → {merged_path}")
        return merged_path

    # ---------- Load to Postgres ----------
    @task()
    def load_csv_to_pg(conn_id: str, csv_path: str, table: str = TARGET_TABLE, append: bool = True) -> int:
        schema = SCHEMA_NAME
        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames
            rows = [tuple((r.get(col, "") or None) for col in fieldnames) for r in reader]

        if not rows:
            print("⚠️ No rows found in CSV; nothing to insert.")
            return 0

        create_schema = f"CREATE SCHEMA IF NOT EXISTS {schema};"
        create_table = f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table} (
                {', '.join([f'{col} TEXT' for col in fieldnames])}
            );
        """
        delete_rows = f"DELETE FROM {schema}.{table};" if not append else None
        insert_sql = f"""
            INSERT INTO {schema}.{table} ({', '.join(fieldnames)})
            VALUES ({', '.join(['%s' for _ in fieldnames])});
        """

        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(create_schema)
                cur.execute(create_table)
                if delete_rows:
                    cur.execute(delete_rows)
                cur.executemany(insert_sql, rows)
                conn.commit()
            print(f"Inserted {len(rows)} rows into {schema}.{table}")
            return len(rows)
        except DatabaseError as e:
            print(f"Database error: {e}")
            conn.rollback()
            return 0
        finally:
            conn.close()

    # ---------- Analysis ----------
    @task()
    def analyze_from_db(conn_id: str = "postgres_default") -> None:
        hook = PostgresHook(postgres_conn_id=conn_id)
        sql = """
            SELECT city, ROUND(AVG(NULLIF(review_stars, '')::numeric), 2) AS avg_stars
            FROM week10_assignment.yelp_merged
            WHERE city IS NOT NULL AND city <> ''
            GROUP BY city
            ORDER BY avg_stars DESC
            LIMIT 10;
        """
        results = hook.get_records(sql)
        print("\nTop 10 Cities by Average Stars:\n")
        for city, avg in results:
            print(f"{city:<20}  {avg}")
        print("\n Analysis complete.")


        df = hook.get_pandas_df(sql)
        # Create output directory (shared between host and container)
        output_dir = "/opt/airflow/data/outputs"
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, "avg_stars_by_city.png")

        # --- Plot ---
        plt.figure(figsize=(10, 6))
        plt.bar(df["city"], df["avg_stars"], color="skyblue", edgecolor="black")
        plt.xticks(rotation=45, ha="right")
        plt.xlabel("City")
        plt.ylabel("Average Stars")
        plt.title("Top 10 Cities by Average Yelp Review Stars")
        plt.tight_layout()
        plt.savefig(output_path)
        plt.close()

        print(f"Visualization saved at: {output_path}")
        return output_path

    @task()
    def clear_intermediate_folder(folder_path: str = "/opt/airflow/data/intermediate") -> None:
        """
        Delete all files and subdirectories inside a specific folder (default: intermediate).
        Keeps the folder itself intact.
        """
        if not os.path.exists(folder_path):
            print(f"Folder {folder_path} does not exist.")
            return

        for filename in os.listdir(folder_path):
            file_path = os.path.join(folder_path, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.remove(file_path)
                    print(f"Removed file: {file_path}")
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
                    print(f"Removed directory: {file_path}")
            except Exception as e:
                print(f"Failed to delete {file_path}: {e}")

        print("Intermediate cleanup complete!")


    # ---------- Task Flow ----------
    business_file = fetch_business()
    review_file = fetch_review()
    merged_path = merge_csvs(business_file, review_file)

    load_to_database = load_csv_to_pg(
        conn_id="postgres_default",
        csv_path=merged_path,
        table=TARGET_TABLE,
    )

    analysis_task = analyze_from_db()

    [business_file, review_file] >> merged_path >> load_to_database >> analysis_task >> clear_intermediate_folder()

