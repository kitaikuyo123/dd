"""
生成 MiniSQL 批量 INSERT 测试 SQL 文件。

用法:
  python tests/sql/gen_insert_sql.py 10000
  # 生成 tests/sql/insert_10000.sql

然后在 CLI 中:
  source tests/sql/insert_10000.sql
"""
import sys, os

def main():
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 100
    outdir = os.path.dirname(os.path.abspath(__file__))
    outfile = os.path.join(outdir, f"insert_{n}.sql")

    with open(outfile, "w", encoding="utf-8") as f:
        f.write("DROP TABLE IF EXISTS insert_test;\n")
        f.write("CREATE TABLE insert_test (id INT PRIMARY KEY, name STRING, value DOUBLE);\n")
        for i in range(1, n + 1):
            f.write(f"INSERT INTO insert_test (id, name, value) VALUES ({i}, 'user_{i}', {i}.5);\n")
        f.write(f"SELECT COUNT(*) AS total FROM insert_test;\n")
        f.write("SELECT * FROM insert_test LIMIT 5;\n")
        f.write("DROP TABLE insert_test;\n")

    print(f"Generated: {outfile} ({n} rows)")

if __name__ == "__main__":
    main()
