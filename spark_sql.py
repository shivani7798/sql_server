"""
spark_sql.py
------------
Reads CSV files from the data/ directory, registers them as Spark SQL
tables, executes the queries defined in queries.sql, and prints a
catalogue of all available tables so you can explore the database.

Usage:
    python spark_sql.py
"""

import glob
import os
import re

from pyspark.sql import SparkSession

# Allow only simple identifiers to prevent SQL injection via table names
_SAFE_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def create_spark_session(app_name: str = "SQLServer") -> SparkSession:
    """Create (or reuse) a local SparkSession."""
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.repl.eagerEval.enabled", "true")
        .getOrCreate()
    )


def load_csv_files(spark: SparkSession, data_dir: str = "data") -> None:
    """
    Discover every CSV file under *data_dir* and register each one as a
    Spark SQL temporary view whose name is derived from the file name
    (without the .csv extension).
    """
    csv_files = glob.glob(os.path.join(data_dir, "*.csv"))
    if not csv_files:
        print(f"[INFO] No CSV files found in '{data_dir}/'.")
        return

    for path in csv_files:
        table_name = os.path.splitext(os.path.basename(path))[0]
        if not _SAFE_IDENTIFIER.match(table_name):
            print(f"[WARN] Skipping '{path}': file name '{table_name}' is not "
                  "a valid SQL identifier (use letters, digits, underscores).")
            continue
        df = spark.read.csv(path, header=True, inferSchema=True)
        df.createOrReplaceTempView(table_name)
        print(f"[INFO] Registered '{path}' as SQL table '{table_name}'")


def show_tables(spark: SparkSession) -> None:
    """Print all tables currently registered in the default database."""
    print("\n=== Available tables ===")
    tables = spark.catalog.listTables()
    if not tables:
        print("  (no tables registered)")
    for tbl in tables:
        print(f"  • {tbl.name}  (database: {tbl.database or 'default'}, "
              f"type: {tbl.tableType})")
    print("========================\n")


def describe_tables(spark: SparkSession) -> None:
    """Show schema and a preview of every registered table."""
    for tbl in spark.catalog.listTables():
        name = tbl.name
        # Table names come from createOrReplaceTempView (validated on
        # registration) but we re-check before interpolation to be safe.
        if not _SAFE_IDENTIFIER.match(name):
            print(f"[WARN] Skipping DESCRIBE for unexpected table name: {name!r}")
            continue
        print(f"\n--- Schema: {name} ---")
        spark.sql(f"DESCRIBE `{name}`").show(truncate=False)
        print(f"--- Preview: {name} (first 5 rows) ---")
        spark.sql(f"SELECT * FROM `{name}` LIMIT 5").show(truncate=False)


def run_sql_file(spark: SparkSession, sql_file: str) -> None:
    """
    Execute all non-empty, non-comment SQL statements found in *sql_file*.
    Results of SELECT statements are printed to stdout.
    """
    if not os.path.isfile(sql_file):
        print(f"[WARN] SQL file not found: {sql_file}")
        return

    with open(sql_file, "r") as fh:
        content = fh.read()

    # Split on semicolons; skip blank lines and comment-only blocks
    statements = [
        stmt.strip()
        for stmt in content.split(";")
        if stmt.strip() and not all(
            line.startswith("--") or not line.strip()
            for line in stmt.strip().splitlines()
        )
    ]

    if not statements:
        print(f"[INFO] No executable statements found in '{sql_file}'.")
        return

    print(f"\n=== Running {sql_file} ===")
    for stmt in statements:
        print(f"\n> {stmt[:120]}{'...' if len(stmt) > 120 else ''}")
        try:
            result = spark.sql(stmt)
            # Only show output for statements that return rows
            if result.columns:
                result.show(truncate=False)
        except Exception as exc:
            print(f"[ERROR] {exc}")
    print(f"=== Done: {sql_file} ===\n")


def main() -> None:
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # 1. Load all CSV files from data/ as SQL tables
    load_csv_files(spark, data_dir="data")

    # 2. Show the database catalogue
    show_tables(spark)

    # 3. Describe each table (schema + preview)
    describe_tables(spark)

    # 4. Execute user queries from queries.sql
    run_sql_file(spark, "queries.sql")

    spark.stop()


if __name__ == "__main__":
    main()
