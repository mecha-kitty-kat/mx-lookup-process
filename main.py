import csv
import requests
import os
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from google.cloud import storage

MAX_WORKERS = 10
MAX_OUTSTANDING = 2000 

def fetch_mx_records(domain):
    url = f"https://dns.google/resolve?name={domain}&type=mx"
    try:
        response = requests.get(url, timeout=5)
        response.raise_for_status()
        data = response.json()
        return data.get("Answer", [])
    except Exception:
        return []

def get_email_provider(mx_records):
    for record in mx_records:
        if "data" in record and isinstance(record["data"], str):
            data = record["data"]
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
    domain = email.split("@")[-1] if "@" in email else ""
    if domain:
        mx_records = fetch_mx_records(domain)
        return get_email_provider(mx_records)
    return "Invalid-Email"

def upload_progress(bucket_name, progress_blob, percent):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(progress_blob)
    blob.upload_from_string(json.dumps({"progress": percent}), content_type="application/json")


# def process_csv(input_file, bucket_name, progress_blob):
#     print("starting file processing...")
#     temp_file = input_file + ".tmp"
#     with open(input_file, mode="r", newline="", encoding="utf-8") as infile:
#         reader = csv.DictReader(infile)
#         raw_fieldnames = reader.fieldnames or []
#         fieldnames = [f.lower() for f in raw_fieldnames]

#         if "email_host" not in fieldnames:
#             fieldnames.append("email_host")

#         # Normalize row keys to lowercase
#         rows = [{k.lower(): v for k, v in row.items()} for row in reader]
#         total = len(rows)
#     print(f"Total rows to process: {total} /n starting email processing...")

#     with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor, \
#             open(temp_file, mode="w", newline="", encoding="utf-8") as outfile:

#         writer = csv.DictWriter(outfile, fieldnames=fieldnames)
#         writer.writeheader()

#         futures = {}
#         for row in rows:
#             if not row.get("email_host"):
#                 email = row.get("email", "")
#                 futures[executor.submit(process_domain, email)] = row
#             else:
#                 writer.writerow(row)

#         completed = 0
#         last_percent = 0

#         with tqdm(total=len(futures), desc="Processing Emails") as pbar:
#             for future in as_completed(futures):
#                 row = futures[future]
#                 row["email_host"] = future.result()
#                 writer.writerow(row)
#                 completed += 1
#                 pbar.update(1)

#                 percent = int((completed / total) * 100)
#                 if percent >= last_percent + 5:
#                     upload_progress(bucket_name, progress_blob, percent)
#                     last_percent = percent

#                 if pbar.n % 50 == 0:
#                     outfile.flush()
#                     os.fsync(outfile.fileno())

#     os.replace(temp_file, input_file)
#     upload_progress(bucket_name, progress_blob, 100)
#     print("CSV processing complete.")

def process_csv(input_file, bucket_name, progress_blob):
    print("starting file processing...", flush=True)

    # Count rows first
    with open(input_file, "r", encoding="utf-8", newline="") as f:
        total = sum(1 for _ in f) - 1
    if total < 0: total = 0
    print(f"Total rows to process: {total}\n starting email processing...", flush=True)

    temp_file = input_file + ".tmp"
    completed = 0
    last_percent = 0

    def normalize_row(row):
        return {k.lower(): v for k, v in row.items()}

    def process_row(row):
        email = row.get("email", "")
        host = row.get("email_host")
        if host and host.strip():
            return row
        provider = process_domain(email)
        row["email_host"] = provider
        return row

    with open(input_file, "r", encoding="utf-8", newline="") as infile, \
         open(temp_file, "w", encoding="utf-8", newline="") as outfile, \
         ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex, \
         tqdm(total=total, desc="Processing Emails", unit="row") as pbar:

        reader = csv.DictReader(infile)
        raw_fields = reader.fieldnames or []
        fieldnames = [f.lower() for f in raw_fields]
        if "email_host" not in fieldnames:
            fieldnames.append("email_host")
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()

        futures = []
        for row in reader:
            row = normalize_row(row)

            if (row.get("email_host") or "").strip():
                writer.writerow(row)
                completed += 1
                pbar.update(1)
            else:
                futures.append(ex.submit(process_row, row))
                if len(futures) >= MAX_OUTSTANDING:
                    for fut in as_completed(futures[:MAX_WORKERS]):
                        out_row = fut.result()
                        writer.writerow(out_row)
                        completed += 1
                        pbar.update(1)
                    futures = [f for f in futures if not f.done()]

            percent = int(completed * 100 / max(1, total))
            if percent >= last_percent + 5:
                try:
                    upload_progress(bucket_name, progress_blob, percent)
                except Exception as e:
                    print(f"⚠️ progress upload failed: {e}", flush=True)
                last_percent = percent

            if completed % 2000 == 0:
                outfile.flush()
                os.fsync(outfile.fileno())

        # Drain remaining
        for fut in as_completed(futures):
            out_row = fut.result()
            writer.writerow(out_row)
            completed += 1
            pbar.update(1)
            if completed % 2000 == 0:
                outfile.flush()
                os.fsync(outfile.fileno())

    os.replace(temp_file, input_file)
    try:
        upload_progress(bucket_name, progress_blob, 100)
    except Exception as e:
        print(f"⚠️ final progress upload failed: {e}", flush=True)
    print("CSV processing complete.", flush=True)

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

def split_and_upload_csv_chunks(csv_path, bucket_name, filename):
    from datetime import datetime

    # Read rows
    with open(csv_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        headers = reader.fieldnames
        if "email_host" not in headers:
            print("❌ 'email_host' header missing in CSV. Exiting.")
            return
        rows = list(reader)

    today_str = datetime.now().strftime("%Y-%m-%d")
    chunk_size = 40000

    # Separate rows
    google_rows = [r for r in rows if r["email_host"] == "Google"]
    others_rows = [r for r in rows if r["email_host"] not in ("Google", "No-Email")]

    client = storage.Client()
    bucket = client.bucket(bucket_name)

    def upload_chunks(rows, category, letter):
        for i in range(0, len(rows), chunk_size):
            chunk = rows[i:i+chunk_size]
            list_number = (i // chunk_size) + 1
            chunk_filename = f"{list_number}_{today_str}_{filename}_{letter}_{len(chunk)}.csv"
            blob_path = f"processed/{filename}/{category}/{chunk_filename}"

            # Write to temp file
            temp_path = f"/tmp/{chunk_filename}"
            with open(temp_path, "w", newline='', encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=headers)
                writer.writeheader()
                writer.writerows(chunk)

            # Upload to GCS
            blob = bucket.blob(blob_path)
            blob.upload_from_filename(temp_path)
            print(f"✅ Uploaded {blob_path}")
            os.remove(temp_path)

    upload_chunks(google_rows, "Google", "G")
    upload_chunks(others_rows, "Others", "O")

def main():
    bucket_name = os.getenv("GCS_BUCKET")
    input_blob = os.getenv("INPUT_CSV")

    if not bucket_name or not input_blob:
        print("Missing GCS_BUCKET or INPUT_CSV environment variable.")
        return

    local_file = "/tmp/input.csv"
    download_blob(bucket_name, input_blob, local_file)

    filename = os.path.basename(input_blob)
    progress_blob = f"processing/{filename}.progress.json"
    process_csv(local_file, bucket_name, progress_blob)

    # output_blob = f"processed/{filename}"
    # upload_blob(bucket_name, local_file, output_blob)
    # split_and_upload_csv_chunks(local_file, bucket_name, os.path.splitext(filename)[0])

if __name__ == "__main__":
    main()
