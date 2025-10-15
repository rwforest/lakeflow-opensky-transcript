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

Streams call center transcripts from HuggingFace datasets, simulating real-time conversation streaming.

**Usage:**
```python
df = spark.readStream.format("transcript") \
    .option("dataset", "AIxBlock/92k-real-world-call-center-scripts-english") \
    .option("utterance_delay", "0.1") \
    .option("loop", "true") \
    .load()
```

**Options:**
- `dataset`: HuggingFace dataset name (default: AIxBlock/92k-real-world-call-center-scripts-english)
- `utterance_delay`: Delay in seconds between utterances (default: 0.1)
- `conversation_delay`: Delay in seconds between conversations (default: 1.0)
- `loop`: Whether to loop continuously through conversations (default: true)
- `max_utterances`: Maximum utterances to stream, -1 for unlimited (default: -1)

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
- Simulates real-time streaming by chunking conversations into utterances
- Supports HuggingFace datasets (tries to load, falls back to sample data)
- Continuous looping for long-running pipelines
- Configurable streaming rate
- Word-level timing information
- PII-redacted transcripts

**How It Works:**

1. **Load Conversations**: Loads call center conversations from HuggingFace or generates sample data
2. **Parse Utterances**: Splits conversations into individual utterances (speaker turns)
3. **Stream Chunks**: Returns utterances in batches (default 10 per batch)
4. **Simulate Timing**: Adds realistic delays between utterances
5. **Loop Continuously**: When enabled, restarts from the beginning after all conversations

This approach reverses the typical streaming pipeline:
- **Normal**: Real-time audio → ASR → streaming utterances → storage
- **Simulated**: Static transcripts → chunking → streaming utterances → pipeline

Perfect for testing and demonstrating streaming pipelines without live data!

---

## Installation

### HuggingFace Datasets (Optional)

To load actual HuggingFace datasets, install the datasets library:

```bash
pip install datasets
```

If not installed, the transcript data source will fall back to realistic sample data.

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

# Stream transcripts
transcripts = spark.readStream.format("transcript") \
    .option("utterance_delay", "0.5") \
    .load()

# Process both streams
flights.writeStream.format("delta").table("flights")
transcripts.writeStream.format("delta").table("transcripts")
```

---

## Notes

- Both data sources follow Spark's Python Data Source API (PySpark 3.4+)
- Transcript data source is designed for simulation/testing purposes
- OpenSky data source requires network access and respects API rate limits
- Both support continuous streaming for long-running pipelines
