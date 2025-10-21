# Custom Spark Data Sources

This directory contains custom Spark data sources for the OpenSky pipeline project.

## Available Data Sources

### 1. OpenSky Network Data Source (`opensky.py`)

Streams real-time aircraft tracking data from the OpenSky Network API.

**Usage:**
```python
df = spark.readStream.format("opensky") \
    .option("region", "EUROPE") \
    .option("client_id", "your_client_id") \
    .option("client_secret", "your_client_secret") \
    .load()
```

**Options:**
- `region`: Geographic region (EUROPE, NORTH_AMERICA, ASIA, etc.)
- `client_id`: OAuth2 client ID for authenticated access
- `client_secret`: OAuth2 client secret

**Features:**
- Real-time aircraft position streaming
- OAuth2 authentication support
- Rate limiting and retry logic
- Multiple geographic regions

---

### 2. Transcript Data Source (`transcript.py`)

Streams call center transcripts from a local FastAPI backend, simulating real-time conversation streaming.

**Prerequisites:**
Start the local API server first:
```bash
cd api
python main.py
```

**Usage:**
```python
# Basic usage - connects to http://localhost:8000 by default
df = spark.readStream.format("transcript").load()

# With custom API configuration
df = spark.readStream.format("transcript") \
    .option("api_base_url", "http://localhost:8000") \
    .option("batch_size", "100") \
    .option("request_delay", "0.05") \
    .load()
```

**Options:**
- `api_base_url`: Base URL of the transcript API (default: http://localhost:8000)
- `batch_size`: Number of utterances to fetch per request (default: 100)
- `request_delay`: Delay between API requests in seconds (default: 0.05)

**Schema:**
```
timestamp: TimestampType          # When the utterance was ingested
conversation_id: StringType       # Unique conversation identifier
utterance_id: IntegerType        # Sequential utterance number
speaker: StringType              # Speaker role (agent/customer)
text: StringType                 # The utterance text
confidence: DoubleType           # ASR confidence score
start_time: DoubleType          # Start time within conversation (seconds)
end_time: DoubleType            # End time within conversation (seconds)
domain: StringType              # Call domain/category
topic: StringType               # Inbound/outbound classification
accent: StringType              # Speaker accent
```

**Features:**
- Polls local API for continuous transcript streaming
- Automatic looping through dataset for infinite streaming
- High throughput capable of handling 500k+ calls/day
- Configurable batch sizes for throughput tuning
- Word-level timing information
- PII-redacted transcripts

**How It Works:**

1. **Local API**: FastAPI server reads from `data/conversations.jsonl` (or generates sample data)
2. **Continuous Polling**: Spark data source polls `/transcript/utterances` endpoint
3. **Automatic Looping**: API automatically loops through the dataset infinitely
4. **Batch Processing**: Returns utterances in configurable batch sizes (default 100)
5. **Rate Control**: Configurable delay between API requests for throughput tuning

Architecture:
```
FastAPI (data/conversations.jsonl) → HTTP API → Spark Streaming → Delta Tables
```

This approach simulates real-time call center data:
- **Normal**: Real-time audio → ASR → streaming utterances → storage
- **Simulated**: Local dataset → API → streaming utterances → pipeline

Perfect for testing and demonstrating streaming pipelines with realistic data locally!

---

## Installation

### Running the Local API

The transcript data source requires the local FastAPI server to be running:

```bash
# Navigate to the API directory
cd api

# Install dependencies (if needed)
pip install -r requirements.txt

# Start the API server
python main.py
```

The API will start on `http://localhost:8000` and serve transcript data from `data/conversations.jsonl`. If the data file is not present, it will generate sample data automatically.

---

## Project Structure

```
dab/src/opensky-pipeline/
├── __init__.py
├── datasources/
│   ├── __init__.py              # Exports both data sources
│   ├── opensky.py               # OpenSky Network data source
│   ├── transcript.py            # Transcript data source
│   └── README.md                # This file
└── transformations/
    ├── __init__.py
    ├── ingest_flights.py        # OpenSky ingestion pipeline
    └── ingest_transcripts.py    # Transcript ingestion pipeline
```

---

## Example: Running Both Pipelines

```python
# Register both data sources
from datasources import OpenSkyDataSource, TranscriptDataSource
spark.dataSource.register(OpenSkyDataSource)
spark.dataSource.register(TranscriptDataSource)

# Stream flights
flights = spark.readStream.format("opensky") \
    .option("region", "NORTH_AMERICA") \
    .load()

# Stream transcripts (requires API running at localhost:8000)
transcripts = spark.readStream.format("transcript") \
    .option("batch_size", "100") \
    .load()

# Process both streams
flights.writeStream.format("delta").table("flights")
transcripts.writeStream.format("delta").table("transcripts")
```

---

## Notes

- Both data sources follow Spark's Python Data Source API (PySpark 3.4+)
- Transcript data source connects to local FastAPI backend for data consumption
- OpenSky data source requires network access and respects API rate limits
- Both support continuous streaming for long-running pipelines
- The transcript API automatically loops through data for infinite streaming
- Data is served from `api/data/conversations.jsonl` or sample data if not present
