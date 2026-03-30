import argparse
import time
import requests
import random
import string
import sys

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

def execute_sql(host, sql):
    url = f"{host}/monitor/api/sql/execute"
    headers = {"Content-Type": "text/plain; charset=UTF-8"}
    try:
        response = requests.post(url, headers=headers, data=sql.encode('utf-8'))
        if response.status_code == 200:
            return True, response.json()
        else:
            return False, response.text
    except Exception as e:
        return False, str(e)

def main():
    parser = argparse.ArgumentParser(description="Continuous data writer for MiniSQL (Load Testing)")
    parser.add_argument("--table", type=str, default="load_test_table", help="Target table name")
    parser.add_argument("--size-mb", type=float, default=1.0, help="Data size per insert in MB (Note: Keep below 4MB to avoid gRPC limits, unless configured otherwise)")
    parser.add_argument("--total-mb", type=float, default=None, help="Stop inserting after reaching this total amount of MB")
    parser.add_argument("--interval", type=float, default=1.0, help="Interval between inserts in seconds")
    parser.add_argument("--host", type=str, default="http://localhost:16010", help="MiniSQL Master Monitor URL")
    
    args = parser.parse_args()
    
    print(f"[*] Assuring table '{args.table}' exists...")
    setup_sql = f"CREATE TABLE {args.table} (id INT PRIMARY KEY, content STRING);"
    success, res = execute_sql(args.host, setup_sql)
    if success:
        print(f"[*] Table '{args.table}' created.")
    else:
        print(f"[*] Warning during table creation (might already exist): {res}")
        
    print(f"[*] Starting continuous writer...")
    print(f"    - Table: {args.table}")
    print(f"    - Payload Size: {args.size_mb} MB per request")
    if args.total_mb:
        print(f"    - Target Total Size: {args.total_mb} MB")
    print(f"    - Frequency: Every {args.interval} seconds")
    print(f"    - Target: {args.host}")
    print(f"[*] Press Ctrl+C to stop.\n")
    
    counter = int(time.time()) # Use timestamp as starting ID to avoid collision on restart
    start_id = counter
    total_inserted_mb = 0.0
    
    try:
        while True:
            if args.total_mb and total_inserted_mb >= args.total_mb:
                print(f"[*] Reached target total data size of {args.total_mb} MB. Stopping.")
                break
                
            payload = generate_random_string(args.size_mb)
            
            insert_sql = f"INSERT INTO {args.table} (id, content) VALUES ({counter}, '{payload}');"
            start_time = time.time()
            success, res = execute_sql(args.host, insert_sql)
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
    sys.exit(0)

if __name__ == "__main__":
    main()
