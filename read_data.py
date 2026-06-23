try:
    import duckdb  # type: ignore[import]
except ImportError:
    import subprocess
    import sys
    subprocess.check_call([sys.executable, "-m", "pip", "install", "duckdb"])
    import duckdb  # type: ignore[import]

# Connect to DuckDB and run a SQL query to read ALL the parquet files at once
con = duckdb.connect()
df = con.execute(
    "SELECT * FROM read_parquet('data_lake/**/*.parquet') ORDER BY Date DESC LIMIT 10").df()

# Print the last 10 days of data!
print(df.to_string())
