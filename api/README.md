# FastAPI Testing API for Spark Data Sources

A REST API for testing the custom Spark data sources (OpenSky and Transcript) without needing a full Spark cluster.

## Features

- **OpenSky Data Source Testing**: Test aircraft tracking data streaming
- **Transcript Data Source Testing**: Test call center transcript streaming
- **Real-time Streaming**: Server-Sent Events (SSE) for simulating live data
- **Analytics Endpoints**: Quick insights into data distribution
- **Interactive Documentation**: Auto-generated Swagger UI
- **No Spark Required**: Standalone testing without Spark dependencies

## Quick Start

### 1. Installation

```bash
cd api
pip install -r requirements.txt
```

### 2. Run the Server

```bash
uvicorn main:app --reload
```

The API will be available at:
- **API**: http://localhost:8000
- **Interactive Docs**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc

### 3. Test the Endpoints

Open your browser to http://localhost:8000/docs to see the interactive API documentation.

---

## API Endpoints

### General

#### `GET /`
Root endpoint with API information and available endpoints.

**Response:**
```json
{
  "message": "Spark Data Sources Testing API",
  "version": "1.0.0",
  "data_sources": ["opensky", "transcript"],
  "documentation": "/docs"
}
```

#### `GET /health`
Health check endpoint.

---

### OpenSky Data Source

#### `GET /opensky/status`
Get the status of the OpenSky data source.

**Response:**
```json
{
  "name": "opensky",
  "available": true,
  "message": "OpenSky data source is available (simulated)",
  "records_available": 50
}
```

#### `GET /opensky/flights`
Get current flights.

**Parameters:**
- `region` (optional): Geographic region (EUROPE, NORTH_AMERICA, ASIA, etc.)
- `limit` (optional): Number of flights to return (1-100, default: 10)

**Example:**
```bash
curl "http://localhost:8000/opensky/flights?region=EUROPE&limit=5"
```

**Response:**
```json
[
  {
    "time_ingest": "2025-10-15T00:00:00Z",
    "icao24": "a12b34",
    "callsign": "UAL123",
    "origin_country": "United States",
    "longitude": -122.4194,
    "latitude": 37.7749,
    "geo_altitude": 10000,
    "on_ground": false,
    "velocity": 450.5,
    "true_track": 180.0,
    "vertical_rate": 0.5
  }
]
```

#### `GET /opensky/stream`
Stream flights in real-time using Server-Sent Events.

**Parameters:**
- `region` (optional): Geographic region
- `interval` (optional): Seconds between updates (0.1-10.0, default: 1.0)
- `duration` (optional): Stream duration in seconds (5-300, default: 30)

**Example:**
```bash
curl -N "http://localhost:8000/opensky/stream?interval=2&duration=10"
```

**Response (SSE):**
```
data: {"timestamp": "2025-10-15T00:00:00Z", "count": 5, "flights": [...]}

data: {"timestamp": "2025-10-15T00:00:02Z", "count": 5, "flights": [...]}

data: {"status": "complete"}
```

---

### Transcript Data Source

#### `GET /transcript/status`
Get the status of the Transcript data source.

**Response:**
```json
{
  "name": "transcript",
  "available": true,
  "message": "Transcript data source is available (simulated)",
  "records_available": 200
}
```

#### `GET /transcript/utterances`
Get next batch of utterances.

**Parameters:**
- `limit` (optional): Number of utterances to return (1-100, default: 10)

**Example:**
```bash
curl "http://localhost:8000/transcript/utterances?limit=5"
```

**Response:**
```json
[
  {
    "timestamp": "2025-10-15T00:00:00Z",
    "conversation_id": "conv_00001",
    "utterance_id": 0,
    "speaker": "agent",
    "text": "Thank you for calling customer support. How may I help you today?",
    "confidence": 0.98,
    "start_time": 0.0,
    "end_time": 3.5,
    "domain": "customer_service",
    "topic": "inbound",
    "accent": "american"
  }
]
```

#### `GET /transcript/conversation/{conversation_id}`
Get all utterances for a specific conversation.

**Example:**
```bash
curl "http://localhost:8000/transcript/conversation/conv_00001"
```

#### `GET /transcript/stream`
Stream transcript utterances in real-time.

**Parameters:**
- `utterance_delay` (optional): Delay between utterances in seconds (0.1-5.0, default: 0.5)
- `max_utterances` (optional): Total utterances to stream (5-500, default: 50)

**Example:**
```bash
curl -N "http://localhost:8000/transcript/stream?utterance_delay=1&max_utterances=20"
```

**Response (SSE):**
```
data: {"timestamp": "...", "utterance": {...}, "progress": "1/20"}

data: {"timestamp": "...", "utterance": {...}, "progress": "2/20"}

data: {"status": "complete", "total": 20}
```

---

### Analytics

#### `GET /transcript/analytics/speaker-distribution`
Get distribution of utterances by speaker (agent vs customer).

**Example:**
```bash
curl "http://localhost:8000/transcript/analytics/speaker-distribution"
```

**Response:**
```json
{
  "total_utterances": 200,
  "agent_utterances": 102,
  "customer_utterances": 98,
  "agent_percentage": 51.0,
  "customer_percentage": 49.0
}
```

#### `GET /transcript/analytics/domain-distribution`
Get distribution of conversations by domain.

**Example:**
```bash
curl "http://localhost:8000/transcript/analytics/domain-distribution"
```

**Response:**
```json
{
  "total_utterances": 200,
  "domains": {
    "billing": 50,
    "technical_support": 75,
    "sales": 40,
    "customer_service": 35
  }
}
```

---

## Testing Examples

### Using cURL

#### Test OpenSky Flights
```bash
# Get 10 flights from Europe
curl "http://localhost:8000/opensky/flights?region=EUROPE&limit=10"

# Stream flights for 30 seconds
curl -N "http://localhost:8000/opensky/stream?duration=30"
```

#### Test Transcripts
```bash
# Get 5 utterances
curl "http://localhost:8000/transcript/utterances?limit=5"

# Stream 20 utterances with 1 second delay
curl -N "http://localhost:8000/transcript/stream?utterance_delay=1&max_utterances=20"

# Get analytics
curl "http://localhost:8000/transcript/analytics/speaker-distribution"
```

### Using Python

```python
import requests
import json

# Test OpenSky
response = requests.get("http://localhost:8000/opensky/flights", params={"limit": 5})
flights = response.json()
print(f"Got {len(flights)} flights")

# Test Transcripts
response = requests.get("http://localhost:8000/transcript/utterances", params={"limit": 10})
utterances = response.json()
print(f"Got {len(utterances)} utterances")

# Stream transcripts (SSE)
with requests.get(
    "http://localhost:8000/transcript/stream",
    params={"utterance_delay": 0.5, "max_utterances": 10},
    stream=True
) as response:
    for line in response.iter_lines():
        if line and line.startswith(b"data: "):
            data = json.loads(line[6:])
            print(data)
```

### Using JavaScript (Browser)

```javascript
// Stream flights using EventSource (SSE)
const flightsSource = new EventSource(
  'http://localhost:8000/opensky/stream?duration=10'
);

flightsSource.onmessage = (event) => {
  const data = JSON.parse(event.data);
  console.log('Flights:', data);
};

// Stream transcripts
const transcriptsSource = new EventSource(
  'http://localhost:8000/transcript/stream?max_utterances=20'
);

transcriptsSource.onmessage = (event) => {
  const data = JSON.parse(event.data);
  console.log('Utterance:', data);
};
```

---

## Development

### Running Tests

```bash
# Install test dependencies
pip install pytest pytest-asyncio httpx

# Run tests (if test files exist)
pytest
```

### Custom Configuration

The API can be configured via command-line options:

```bash
# Change host and port
uvicorn main:app --host 0.0.0.0 --port 8080

# Enable auto-reload for development
uvicorn main:app --reload

# Run in production mode
uvicorn main:app --workers 4
```

---

## Architecture

```
api/
├── main.py              # FastAPI application
├── requirements.txt     # Python dependencies
└── README.md           # This file
```

### Key Components

1. **OpenSkySimulator**: Generates realistic flight data
   - 50 sample flights with randomized data
   - Supports region filtering
   - Simulates continuous updates

2. **TranscriptSimulator**: Generates realistic call center conversations
   - 20 sample conversations (~200 utterances)
   - Multiple domains, topics, and accents
   - Simulates streaming utterance by utterance

3. **FastAPI Endpoints**: RESTful API with:
   - Data retrieval endpoints
   - Real-time streaming (SSE)
   - Analytics endpoints
   - Auto-generated documentation

---

## Comparison: API vs Spark Data Sources

| Feature | FastAPI API | Spark Data Sources |
|---------|-------------|-------------------|
| **Purpose** | Testing & Development | Production Streaming |
| **Environment** | Standalone | Spark Cluster |
| **Data** | Simulated Samples | Real/Simulated Data |
| **Streaming** | Server-Sent Events | Spark Structured Streaming |
| **Scale** | Small datasets | Large-scale streaming |
| **Use Case** | Quick testing, demos | Production pipelines |

---

## Next Steps

1. **Test the Endpoints**: Open http://localhost:8000/docs and try the interactive API
2. **Build a Dashboard**: Use the streaming endpoints to create a real-time dashboard
3. **Integration Testing**: Test Spark pipelines against this API
4. **Add Custom Data**: Modify simulators to use your own data

---

## Troubleshooting

### Port Already in Use
```bash
# Use a different port
uvicorn main:app --port 8001
```

### Module Import Errors
```bash
# Ensure you're in the api directory
cd api
python -c "import fastapi; print('FastAPI installed')"
```

### CORS Issues (Browser)
Add CORS middleware to `main.py`:
```python
from fastapi.middleware.cors import CORSMiddleware

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)
```

---

## License

Educational example for testing Spark data sources. See main project README for details.
