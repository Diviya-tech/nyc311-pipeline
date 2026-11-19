import duckdb   # DuckDB: a small database that runs inside Python. No server to install.

# The * is a wildcard. It matches every date folder we have saved so far.
FILES = "data/raw/*/311.parquet"

# duckdb.sql(...) runs SQL text. print(...) shows the result.
# The f before the quotes lets {FILES} be replaced by the path above.
print(duckdb.sql(f"""
    -- Check 1: do the two numbers match? If yes, there are no duplicate rows.
    SELECT COUNT(*)                    AS total_rows,
           COUNT(DISTINCT unique_key)  AS distinct_ids
    FROM '{FILES}'
"""))

print(duckdb.sql(f"""
    -- Check 2: which complaint types are most common?
    SELECT complaint_type,
           COUNT(*) AS n            -- how many rows in each group
    FROM '{FILES}'
    GROUP BY complaint_type         -- one result line per complaint type
    ORDER BY n DESC                 -- biggest first
    LIMIT 5                         -- only the top 5
"""))
