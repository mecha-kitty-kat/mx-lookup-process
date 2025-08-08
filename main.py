import csv
import os
import json
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from tqdm import tqdm
import requests
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from google.cloud import storage

# --- knobs ---
MAX_WORKERS = 10                 # <- hard concurrency limit
IN_FLIGHT = MAX_WORKERS          # keep exactly <= 10 tasks in flight
PROGRESS_STEP = 5                # upload progress every N percent
FLUSH_EVERY = 20000              # flush outfile every N rows

# ---------- HTTP session w/ retries & pooling ----------
def make_session(pool=MAX_WORKERS):
    s = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=0.3,  # 0.3, 0.6, 1.2s
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    a = HTTPAdapter(
        pool_connections=pool,
        pool_maxsize=pool,
        max_retries=retry,
        pool_block=True,
    )
    s.mount("https://", a)
    s.mount("http://", a)
    return s

# ---------- GCS helpers ----------
def upload_progress(bucket_name, progress_blob, percent):
    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(progress_blob)
        blob.upload_from_string(json.dumps({"progress": percent}), content_type="application/json")
    except Exception as e:
        print(f"⚠️ progress upload failed: {e}", flush=True)

def download_blob(bucket_name, source_blob_name, destination_file_name):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)
    print(f"Downloaded {source_blob_name} from {bucket_name}.")

# ---------- MX logic ----------
def fetch_mx_records(domain, session):
    url = f"https://dns.google/resolve?name={domain}&type=mx"
    try:
        r = session.get(url, timeout=(3, 8))  # (connect, read)
        r.raise_for_status()
        return r.json().get("Answer", [])
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

def process_domain(email, session):
    domain = email.split("@")[-1].strip().lower() if "@" in email else ""
    if not domain:
        return "Invalid-Email"
    mx = fetch_mx_records(domain, session)
    return get_email_provider(mx)

# ---------- Processor (streaming, in-flight cap, tqdm) ----------
def process_csv(input_file, bucket_name, progress_blob):
    print("starting file processing...", flush=True)

    # Count rows once (no memory blow-up)
    with open(input_file, "r", encoding="utf-8", newline="") as f:
        total = max(0, sum(1 for _ in f) - 1)
    print(f"Total rows to process: {total}\n starting email processing...", flush=True)

    temp_file = input_file + ".tmp"
    completed = 0
    last_percent = 0
    session = make_session()

    def normalize_row(row):  # lower-case keys
        return {k.lower(): v for k, v in row.items()}

    def worker(row):
        # Only resolve if email_host not already set
        host = (row.get("email_host") or "").strip()
        if host:
            return row
        email = row.get("email", "")
        row["email_host"] = process_domain(email, session)
        return row

    with open(input_file, "r", encoding="utf-8", newline="") as infile, \
         open(temp_file,  "w", encoding="utf-8", newline="") as outfile, \
         ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex, \
         tqdm(total=total, desc="Processing Emails", unit="row") as pbar:

        reader = csv.DictReader(infile)
        raw_fields = reader.fieldnames or []
        fieldnames = [f.lower() for f in raw_fields]
        if "email_host" not in fieldnames:
            fieldnames.append("email_host")
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()

        in_flight = set()

        # Prime pool
        for row in reader:
            row = normalize_row(row)
            if (row.get("email_host") or "").strip():
                writer.writerow(row); completed += 1; pbar.update(1)
            else:
                in_flight.add(ex.submit(worker, row))
            if len(in_flight) >= IN_FLIGHT:
                break

        # Main loop
        for row in reader:
            # wait for at least one to finish, write it
            if in_flight:
                done, in_flight = wait(in_flight, return_when=FIRST_COMPLETED)
                for fut in done:
                    out_row = fut.result()
                    writer.writerow(out_row)
                    completed += 1
                    pbar.update(1)

            # submit current row
            row = normalize_row(row)
            if (row.get("email_host") or "").strip():
                writer.writerow(row); completed += 1; pbar.update(1)
            else:
                in_flight.add(ex.submit(worker, row))

            # progress + occasional flush
            percent = int(completed * 100 / max(1, total))
            if percent >= last_percent + PROGRESS_STEP:
                upload_progress(bucket_name, progress_blob, percent)
                last_percent = percent
            if completed % FLUSH_EVERY == 0:
                outfile.flush()

        # Drain remaining tasks
        while in_flight:
            done, in_flight = wait(in_flight, return_when=FIRST_COMPLETED)
            for fut in done:
                out_row = fut.result()
                writer.writerow(out_row)
                completed += 1
                pbar.update(1)
                if completed % FLUSH_EVERY == 0:
                    outfile.flush()

    os.replace(temp_file, input_file)
    upload_progress(bucket_name, progress_blob, 100)
    print("CSV processing complete.", flush=True)

# ---------- Streaming splitter (no RAM spike, uploads all categories) ----------
def split_and_upload_csv_chunks_stream(csv_path, bucket_name, filename, chunk_size=40000):
    from datetime import datetime
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    today_str = datetime.now().strftime("%Y-%m-%d")

    def flush(category, letter, headers, rows, counters):
        if not rows:
            return
        idx = counters.setdefault(category, 0) + 1
        counters[category] = idx
        size = len(rows)
        chunk_filename = f"{idx}_{today_str}_{filename}_{letter}_{size}.csv"
        blob_path = f"processed/{filename}/{category}/{chunk_filename}"
        temp_path = f"/tmp/{chunk_filename}"
        with open(temp_path, "w", newline='', encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=headers)
            w.writeheader()
            w.writerows(rows)
        bucket.blob(blob_path).upload_from_filename(temp_path)
        os.remove(temp_path)
        print(f"✅ Uploaded {blob_path}")

    g_rows, o_rows, n_rows = [], [], []
    counters = {}

    with open(csv_path, newline='', encoding='utf-8') as f:
        r = csv.DictReader(f)
        headers = r.fieldnames or []
        if "email_host" not in headers:
            print("❌ 'email_host' header missing in CSV. Exiting.")
            return

        for row in r:
            host = (row.get("email_host") or "").strip()
            if host == "Google":
                g_rows.append(row)
                if len(g_rows) >= chunk_size:
                    flush("Google", "G", headers, g_rows, counters); g_rows = []
            elif host == "No-Email":
                n_rows.append(row)
                if len(n_rows) >= chunk_size:
                    flush("NoEmail", "N", headers, n_rows, counters); n_rows = []
            else:
                o_rows.append(row)
                if len(o_rows) >= chunk_size:
                    flush("Others", "O", headers, o_rows, counters); o_rows = []

        # leftovers
        flush("Google", "G", headers, g_rows, counters)
        flush("Others", "O", headers, o_rows, counters)
        flush("NoEmail", "N", headers, n_rows, counters)

    print("Split summary:", counters)

# ---------- main ----------
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

    # Upload split CSVs (Google / Others / NoEmail) in 40k chunks
    split_and_upload_csv_chunks_stream(
        local_file,
        bucket_name,
        os.path.splitext(filename)[0],
        chunk_size=40000,
    )

if __name__ == "__main__":
    main()
