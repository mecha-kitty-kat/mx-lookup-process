import csv
import requests
import os
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from tqdm import tqdm
from google.cloud import storage

MAX_WORKERS = int(os.getenv("MAX_WORKERS", "10"))
REQUEST_TIMEOUT = 5
PROGRESS_PATH = "processing/progress.json"  # fixed path

def fetch_mx_records(domain):
    url = f"https://dns.google/resolve?name={domain}&type=mx"
    try:
        response = requests.get(url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        data = response.json()
        return data.get("Answer", [])
    except Exception:
        return []

def get_email_provider(mx_records):
    for record in mx_records:
        if "data" in record and isinstance(record["data"], str):
            data = record["data"].lower()
            if "google" in data:
                return "Google"
            if "outlook.com" in data or "office365" in data:
                return "Outlook"
            if any(k in data for k in ["pphosted.com", "ppe-hosted", "ppsmtp", "sophos.com"]):
                return "Proofpoint"
            if "mimecast" in data:
                return "Mimecast"
            if "barracuda" in data:
                return "Barracuda"
            if "fortimail" in data or "fortimailcloud.com" in data:
                return "Fortinet"
            if "emailsrvr.com" in data:
                return "Rackspace"
            if "trendmicro.com" in data:
                return "TrendMicro"
            if "securemx" in data:
                return "SecureMX"
            if "mxthunder.net" in data:
                return "MXThunder"
            if "mtaroutes.com" in data:
                return "MTARoutes"
    return "Other" if mx_records else "No-Email"

def process_domain(email):
    domain = email.split("@")[-1].strip().lower() if "@" in email else ""
    if domain:
        mx_records = fetch_mx_records(domain)
        return get_email_provider(mx_records)
    return "Invalid-Email"

def write_progress(client, bucket_name, percent, message):
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(PROGRESS_PATH)
    blob.cache_control = "no-store"
    payload = {
        "percent": int(max(0, min(100, percent))),
        "message": message,
        "updated_at": datetime.now(timezone.utc).isoformat()
    }
    blob.upload_from_string(json.dumps(payload), content_type="application/json")

def process_csv(input_file, bucket_name):
    client = storage.Client()
    write_progress(client, bucket_name, 0, "Starting")

    temp_file = input_file + ".tmp"

    with open(input_file, mode="r", newline="", encoding="utf-8") as infile:
        reader = csv.DictReader(infile)
        fieldnames = reader.fieldnames or []
        if "email_host" not in fieldnames:
            fieldnames.append("email_host")
        rows = list(reader)

    pending_rows = [r for r in rows if not r.get("email_host")]
    total = len(pending_rows)
    processed = 0
    last_percent = -1

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor, \
         open(temp_file, mode="w", newline="", encoding="utf-8") as outfile:

        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()

        futures = {}
        for row in rows:
            if not row.get("email_host"):
                email = row.get("email", "") or ""
                futures[executor.submit(process_domain, email)] = row
            else:
                writer.writerow(row)

        with tqdm(total=len(futures), desc="Processing Emails") as pbar:
            for future in as_completed(futures):
                row = futures[future]
                try:
                    row["email_host"] = future.result()
                except Exception:
                    row["email_host"] = "Error"
                writer.writerow(row)
                pbar.update(1)

                processed += 1
                percent = 100 if total == 0 else (processed * 100) // total
                if percent > last_percent:
                    write_progress(client, bucket_name, percent, f"Processed {processed}/{total}")
                    last_percent = percent

                if pbar.n % 50 == 0:
                    outfile.flush()
                    os.fsync(outfile.fileno())

    os.replace(temp_file, input_file)
    write_progress(client, bucket_name, 100, "Complete")
    print("CSV processing complete.")

def download_blob(bucket_name, source_blob_name, destination_file_name):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)
    print(f"Downloaded {source_blob_name} from {bucket_name}.")

def upload_blob(bucket_name, source_file_name, destination_blob_name):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)
    print(f"Uploaded processed file to {destination_blob_name} in {bucket_name}.")

def main():
    bucket_name = os.getenv("GCS_BUCKET")
    input_blob = os.getenv("INPUT_CSV")

    if not bucket_name or not input_blob:
        print("Missing GCS_BUCKET or INPUT_CSV environment variable.")
        return

    local_file = "/tmp/input.csv"
    download_blob(bucket_name, input_blob, local_file)

    try:
        process_csv(local_file, bucket_name)
    except Exception as e:
        try:
            client = storage.Client()
            write_progress(client, bucket_name, 100, f"Failed: {e}")
        except Exception:
            pass
        raise

    filename = os.path.basename(input_blob)
    output_blob = f"processed/{filename}"
    upload_blob(bucket_name, local_file, output_blob)

if __name__ == "__main__":
    main()
