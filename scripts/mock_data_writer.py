import argparse
import time
import requests
import random
import string
import sys

REQUEST_TIMEOUT_SECONDS = 30
RETRYABLE_STATUS_CODES = {502, 503, 504}

def generate_random_string(size_mb):
    # 1 MB = 1024 * 1024 chars
    size_in_chars = int(size_mb * 1024 * 1024)
    if size_in_chars <= 0:
        return "tiny_payload"
    # To speed up generation, we create a small chunk and multiply it
    chunk_size = 1024
    chunk = ''.join(random.choices(string.ascii_letters + string.digits, k=chunk_size))
    times = size_in_chars // chunk_size
    remainder = size_in_chars % chunk_size
    return chunk * times + chunk[:remainder]

def execute_sql(session, host, sql, timeout=REQUEST_TIMEOUT_SECONDS):
    url = f"{host}/monitor/api/sql/execute"
    headers = {"Content-Type": "text/plain; charset=UTF-8"}
    try:
        response = session.post(url, headers=headers, data=sql.encode('utf-8'), timeout=timeout)
        if response.status_code == 200:
            return True, response.json()
        else:
            return False, response.text
    except Exception as e:
        return False, str(e)

def execute_sql_with_retry(session, host, sql, attempts, retry_delay):
    last_result = (False, "uninitialized")
    for attempt in range(1, attempts + 1):
        success, res = execute_sql(session, host, sql)
        last_result = (success, res)
        if success:
            return last_result

        response_text = str(res)
        retryable = any(str(code) in response_text for code in RETRYABLE_STATUS_CODES)
        if attempt < attempts and retryable:
            time.sleep(retry_delay)
            continue
        return last_result
    return last_result

def is_table_already_exists(response):
    text = str(response).lower()
    return "already exists" in text or "table exists" in text or "exist" in text

def build_payload(base_payload, row_id):
    return f"{base_payload}_{row_id}"

def main():
    parser = argparse.ArgumentParser(description="Continuous data writer for MiniSQL (Load Testing)")
    parser.add_argument("--table", type=str, default="load_test_table", help="Target table name")
    parser.add_argument("--size-mb", type=float, default=1.0, help="Data size per insert in MB (Note: Keep below 4MB to avoid gRPC limits, unless configured otherwise)")
    parser.add_argument("--total-mb", type=float, default=None, help="Stop inserting after reaching this total amount of MB")
    parser.add_argument("--interval", type=float, default=1.0, help="Interval between inserts in seconds")
    parser.add_argument("--host", type=str, default="http://localhost:16010", help="MiniSQL Master Monitor URL")
    parser.add_argument("--timeout", type=float, default=REQUEST_TIMEOUT_SECONDS, help="HTTP timeout in seconds")
    parser.add_argument("--retries", type=int, default=3, help="Retry count for transient HTTP failures")
    parser.add_argument("--retry-delay", type=float, default=1.0, help="Delay between retries in seconds")
    parser.add_argument("--reuse-payload", action="store_true", help="Reuse a pre-generated payload template instead of generating a new large payload every round")
    
    args = parser.parse_args()

    if args.size_mb <= 0:
        print("[ERROR] --size-mb must be > 0")
        sys.exit(1)

    if args.retries <= 0:
        print("[ERROR] --retries must be >= 1")
        sys.exit(1)

    session = requests.Session()
    
    print(f"[*] Assuring table '{args.table}' exists...")
    setup_sql = f"CREATE TABLE {args.table} (id INT PRIMARY KEY, content STRING);"
    success, res = execute_sql_with_retry(session, args.host, setup_sql, args.retries, args.retry_delay)
    if success:
        print(f"[*] Table '{args.table}' created.")
    elif is_table_already_exists(res):
        print(f"[*] Table '{args.table}' already exists, continuing.")
    else:
        print(f"[ERROR] Failed to assure table '{args.table}' exists: {res}")
        session.close()
        sys.exit(1)
        
    print(f"[*] Starting continuous writer...")
    print(f"    - Table: {args.table}")
    print(f"    - Payload Size: {args.size_mb} MB per request")
    if args.total_mb:
        print(f"    - Target Total Size: {args.total_mb} MB")
    print(f"    - Frequency: Every {args.interval} seconds")
    print(f"    - Target: {args.host}")
    print(f"    - Timeout: {args.timeout} seconds")
    print(f"    - Retries: {args.retries}")
    print(f"    - Reuse Payload: {args.reuse_payload}")
    print(f"[*] Press Ctrl+C to stop.\n")
    
    counter = int(time.time()) # Use timestamp as starting ID to avoid collision on restart
    start_id = counter
    total_inserted_mb = 0.0
    payload_template = generate_random_string(args.size_mb)
    
    try:
        while True:
            if args.total_mb and total_inserted_mb >= args.total_mb:
                print(f"[*] Reached target total data size of {args.total_mb} MB. Stopping.")
                break
                
            if args.reuse_payload:
                payload = build_payload(payload_template, counter)
            else:
                payload = build_payload(generate_random_string(args.size_mb), counter)
            
            insert_sql = f"INSERT INTO {args.table} (id, content) VALUES ({counter}, '{payload}');"
            start_time = time.time()
            success, res = execute_sql_with_retry(session, args.host, insert_sql, args.retries, args.retry_delay)
            latency_ms = (time.time() - start_time) * 1000
            
            if success:
                total_inserted_mb += args.size_mb
                print(f"[SUCCESS] Inserted row ID={counter} | Size: {args.size_mb} MB | Total: {total_inserted_mb:.2f} MB | Latency: {latency_ms:.2f} ms")
            else:
                print(f"[ERROR] Failed to insert row ID={counter} | Error: {res}")
                
            counter += 1
            time.sleep(args.interval)
            
    except KeyboardInterrupt:
        print(f"\n[*] Stopped.")
        
    print(f"[*] Total records inserted in this session: {counter - start_id}")
    print(f"[*] Total data volume inserted: {total_inserted_mb:.2f} MB")
    session.close()
    sys.exit(0)

if __name__ == "__main__":
    main()
