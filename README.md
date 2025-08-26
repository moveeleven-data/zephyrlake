<h1 align="center">Zephyrlake: OpenAQ Batch Pipeline</h1>

<p align="center">
  Fetch OpenAQ sensor readings and save them as daily Parquet files.
  <br/><br/>
</p>

---

Zephyrlake does three things:

- **Retrieve** measurements for a single sensor over a defined date range  
- **Normalize** the raw JSON into a DataFrame  
- **Organize** the results into daily Parquet partitions


## Quickstart

### 1. Create a virtual environment

```bash
# Clone the repo
git clone https://github.com/moveeleven-data/zephyrlake.git
cd zephyrlake

# Set up a virtual environment
python3 -m venv .venv
source .venv/bin/activate   # Linux/Mac
.venv\Scripts\activate      # Windows

# Install dependencies
pip install -r requirements.txt
```


### 2. Set your OpenAQ API key

OpenAQ v3 requires an API key via the `X-API-Key` header. See the [OpenAQ Docs](https://api.openaq.org/).

```bash
# Export directly
export OPENAQ_API_KEY="YOUR_KEY"

# Or create a .env file
printf 'OPENAQ_API_KEY=YOUR_KEY\n' > .env
```

### 3. Run Zephyrlake

To run the pipeline, provide:

- `--sensor` — OpenAQ sensor ID, linked to a monitoring device. See [OpenAQ Docs](https://api.openaq.org/) to look up IDs.  
- `--since` — start date in UTC (YYYY-MM-DD)  
- `--out` — output folder  
- `--pages` — number of pages to fetch  

```bash
PYTHONPATH=src python -m zephyrlake --sensor 359 --since 2025-08-01 --out data/out --pages 3
```

---

### Results

Files are written as Parquet under `data/out/`, with one folder per day (`event_date=YYYY-MM-DD/`).
Each file is named with a deterministic hash of its contents to ensure idempotency.

You should see logs for each page fetched and files written under:

Example run output:

```bash
Fetched 300 rows from sensor 359 since 2025-08-01
Wrote 2 file(s) to data/out across 2 day(s); kept 300/300 rows
```

Output layout:

```bash
data/out/
    event_date=2025-08-01/
        part-a1b2c3d4e5f6.parquet
    event_date=2025-08-02/
        part-f6e5d4c3b2a1.parquet
```

---

<p align="center">
  Built by <a href="https://github.com/moveeleven-data">Matthew Tripodi</a>
</p>

