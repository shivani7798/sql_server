# sql_server

A PySpark + SQL workspace ready to run in GitHub Codespaces.

## Quick start (Codespaces / local)

1. Open the repository in GitHub Codespaces (or clone it locally).  
   The devcontainer installs Python 3.11, Java 17, and all Python
   dependencies automatically.
2. If running locally, install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Place your CSV files in the `data/` directory.  
   A `data/sample.csv` file is included as a starter dataset.
4. Run the Spark script:
   ```bash
   python spark_sql.py
   ```
   The script will:
   - Load every `*.csv` file in `data/` as a Spark SQL table
   - Print a catalogue of all available tables
   - Show the schema and a 5-row preview of each table
   - Execute all queries in `queries.sql`

## Writing SQL

| File | Purpose |
|---|---|
| `queries.sql` | `SELECT` queries executed by `spark_sql.py` |
| `create_tables.sql` | DDL statements (reference / documentation) |
| `insert_data.sql` | DML statements (reference / documentation) |

## Project layout

```
.
├── .devcontainer/
│   └── devcontainer.json   # Codespaces environment (Python 3.11 + Java 17)
├── data/
│   └── sample.csv          # Sample dataset (replace / extend with your own)
├── spark_sql.py            # PySpark entry point
├── requirements.txt        # Python dependencies
├── queries.sql             # SQL queries run by spark_sql.py
├── create_tables.sql       # DDL reference
└── insert_data.sql         # DML reference
```
