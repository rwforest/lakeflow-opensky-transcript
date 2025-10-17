"""
FastAPI Application for OpenSky and Transcript Data Sources

This application provides REST endpoints to access real-time aircraft tracking data
from the OpenSky Network and call center transcripts from local datasets.

Run with:
    uvicorn main:app --reload

Access at:
    http://localhost:8000
    http://localhost:8000/docs (Swagger UI)
"""

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from typing import Optional, List, Dict, Any
from datetime import datetime, timezone
from pydantic import BaseModel, Field
import json
import asyncio
from enum import Enum
import os

# Import real data source components
import sys
import requests
import time
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

app = FastAPI(
    title="OpenSky & Transcript Data API",
    description="Access real-time OpenSky Network data and call center transcript datasets via REST endpoints",
    version="2.0.0"
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============================================================================
# Models
# ============================================================================

class Region(str, Enum):
    """Available regions for OpenSky data"""
    EUROPE = "EUROPE"
    NORTH_AMERICA = "NORTH_AMERICA"
    SOUTH_AMERICA = "SOUTH_AMERICA"
    ASIA = "ASIA"
    AUSTRALIA = "AUSTRALIA"
    AFRICA = "AFRICA"
    GLOBAL = "GLOBAL"


class FlightRecord(BaseModel):
    """Model for OpenSky flight data"""
    time_ingest: datetime
    icao24: str
    callsign: Optional[str]
    origin_country: str
    time_position: Optional[datetime]
    last_contact: Optional[datetime]
    longitude: Optional[float]
    latitude: Optional[float]
    geo_altitude: Optional[float]
    on_ground: Optional[bool]
    velocity: Optional[float]
    true_track: Optional[float]
    vertical_rate: Optional[float]


class TranscriptUtterance(BaseModel):
    """Model for transcript utterance"""
    timestamp: datetime
    conversation_id: str
    utterance_id: int
    speaker: str = Field(description="agent or customer")
    text: str
    confidence: float = Field(ge=0.0, le=1.0)
    start_time: float
    end_time: float
    domain: Optional[str]
    topic: Optional[str]
    accent: Optional[str]


class DataSourceStatus(BaseModel):
    """Status of a data source"""
    name: str
    available: bool
    message: str
    records_available: Optional[int] = None


# ============================================================================
# OpenSky Network Integration
# ============================================================================

class BoundingBox:
    def __init__(self, lamin: float, lamax: float, lomin: float, lomax: float):
        self.lamin = lamin
        self.lamax = lamax
        self.lomin = lomin
        self.lomax = lomax


REGION_BBOXES = {
    "EUROPE": BoundingBox(35.0, 72.0, -25.0, 45.0),
    "NORTH_AMERICA": BoundingBox(7.0, 72.0, -168.0, -60.0),
    "SOUTH_AMERICA": BoundingBox(-56.0, 15.0, -90.0, -30.0),
    "ASIA": BoundingBox(-10.0, 82.0, 45.0, 180.0),
    "AUSTRALIA": BoundingBox(-50.0, -10.0, 110.0, 180.0),
    "AFRICA": BoundingBox(-35.0, 37.0, -20.0, 52.0),
    "GLOBAL": BoundingBox(-90.0, 90.0, -180.0, 180.0),
}


class OpenSkyClient:
    """Client for OpenSky Network API"""

    MIN_REQUEST_INTERVAL = 5.0  # seconds
    MAX_RETRIES = 3

    def __init__(self):
        self.session = self._create_session()
        self.last_request_time = 0
        self.client_id = os.environ.get('OPENSKY_CLIENT_ID')
        self.client_secret = os.environ.get('OPENSKY_CLIENT_SECRET')
        self.access_token = None
        self.token_expires_at = 0

    def _create_session(self) -> requests.Session:
        """Create and configure requests session with retry logic"""
        session = requests.Session()
        retry_strategy = Retry(
            total=self.MAX_RETRIES,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def _get_access_token(self):
        """Get OAuth2 access token using client credentials flow"""
        if not self.client_id or not self.client_secret:
            return

        current_time = time.time()
        if self.access_token and current_time < self.token_expires_at:
            return

        token_url = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
        data = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret
        }

        try:
            response = requests.post(token_url, data=data, timeout=10)
            response.raise_for_status()
            token_data = response.json()

            self.access_token = token_data["access_token"]
            expires_in = token_data.get("expires_in", 1800)
            self.token_expires_at = current_time + expires_in - 300
        except Exception as e:
            print(f"Failed to get access token: {e}")

    def _handle_rate_limit(self):
        """Ensure minimum interval between requests"""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time

        if time_since_last < self.MIN_REQUEST_INTERVAL:
            sleep_time = self.MIN_REQUEST_INTERVAL - time_since_last
            time.sleep(sleep_time)

        self.last_request_time = time.time()

    def get_flights(self, region: str = "NORTH_AMERICA") -> List[Dict[str, Any]]:
        """Fetch flights from OpenSky Network API"""
        self._handle_rate_limit()

        if self.client_id and self.client_secret:
            self._get_access_token()

        bbox = REGION_BBOXES.get(region.upper(), REGION_BBOXES["NORTH_AMERICA"])

        params = {
            'lamin': bbox.lamin,
            'lamax': bbox.lamax,
            'lomin': bbox.lomin,
            'lomax': bbox.lomax
        }

        headers = {}
        if self.access_token:
            headers['Authorization'] = f'Bearer {self.access_token}'

        try:
            response = self.session.get(
                "https://opensky-network.org/api/states/all",
                params=params,
                headers=headers,
                timeout=10
            )
            response.raise_for_status()
            data = response.json()

            flights = []
            timestamp = data.get('time', int(time.time()))

            for state in data.get('states', []):
                if not state or len(state) < 17:
                    continue

                if state[0] is None or state[5] is None or state[6] is None:
                    continue

                flights.append({
                    "time_ingest": datetime.fromtimestamp(timestamp, tz=timezone.utc),
                    "icao24": state[0],
                    "callsign": state[1].strip() if state[1] else None,
                    "origin_country": state[2],
                    "time_position": datetime.fromtimestamp(state[3], tz=timezone.utc) if state[3] else None,
                    "last_contact": datetime.fromtimestamp(state[4], tz=timezone.utc) if state[4] else None,
                    "longitude": float(state[5]) if state[5] is not None else None,
                    "latitude": float(state[6]) if state[6] is not None else None,
                    "geo_altitude": float(state[7]) if state[7] is not None else None,
                    "on_ground": bool(state[8]) if state[8] is not None else None,
                    "velocity": float(state[9]) if state[9] is not None else None,
                    "true_track": float(state[10]) if state[10] is not None else None,
                    "vertical_rate": float(state[11]) if state[11] is not None else None,
                })

            return flights

        except Exception as e:
            raise HTTPException(status_code=503, detail=f"OpenSky API error: {str(e)}")


# ============================================================================
# Transcript Integration
# ============================================================================

class TranscriptClient:
    """Client for call center transcript datasets"""

    LOCAL_DATASET_PATH = "data/conversations.jsonl"
    CHUNK_SIZE = 1000  # Load utterances in chunks

    def __init__(self):
        self.conversations = []
        self.current_idx = 0
        self.total_utterances = 0
        self._load_dataset()

    def _load_dataset(self):
        """Load conversations from local file or use fallback data"""
        # Try to load from local file first (fastest)
        if os.path.exists(self.LOCAL_DATASET_PATH):
            print(f"Loading dataset from local file: {self.LOCAL_DATASET_PATH}")
            self._load_from_local_file()
            return

        # Use sample data if no local file exists
        print("No local dataset file found, using sample data")
        self._generate_sample_data()

    def _load_from_local_file(self):
        """Load all utterances from local JSONL file"""
        import json

        print("Loading utterances from local dataset...")
        with open(self.LOCAL_DATASET_PATH, 'r') as f:
            for line in f:
                utterance = json.loads(line.strip())
                self.conversations.append(utterance)

        self.total_utterances = len(self.conversations)
        print(f"Loaded {self.total_utterances:,} utterances from local dataset")

        # Calculate average conversation length
        conv_ids = set(u['conversation_id'] for u in self.conversations)
        avg_utterances = self.total_utterances / len(conv_ids)
        print(f"Average utterances per conversation: {avg_utterances:.1f}")
        print(f"For 500k calls/day: ~{int(500000 * avg_utterances / 86400)} utterances/second needed")

    def _generate_sample_data(self):
        """Generate sample conversations as fallback"""
        import random

        print("Generating sample call center conversations...")

        domains = ['billing', 'technical_support', 'sales', 'customer_service']
        topics = ['inbound', 'outbound']
        accents = ['indian', 'american', 'filipino']

        templates = [
            [
                ("agent", "Thank you for calling customer support. How may I help you today?"),
                ("customer", "Hi, I'm having trouble with my recent bill."),
                ("agent", "I understand your concern. Let me pull up your account."),
                ("customer", "Sure, my account number is 12345."),
                ("agent", "Thank you. I can see your account now."),
            ],
            [
                ("agent", "Hello, this is tech support. What's the problem?"),
                ("customer", "My internet keeps dropping every few minutes."),
                ("agent", "I'm sorry to hear that. Let's troubleshoot this."),
                ("customer", "Okay, what should I do?"),
                ("agent", "Can you check the lights on your modem?"),
            ],
        ]

        for i in range(50):
            template = random.choice(templates)
            conv_id = f"conv_{i:05d}"
            current_time = 0.0

            for idx, (speaker, text) in enumerate(template):
                word_count = len(text.split())
                duration = word_count * 0.5

                self.conversations.append({
                    "conversation_id": conv_id,
                    "utterance_id": idx,
                    "speaker": speaker,
                    "text": text,
                    "confidence": 0.94 + random.random() * 0.06,
                    "start_time": current_time,
                    "end_time": current_time + duration,
                    "domain": random.choice(domains),
                    "topic": random.choice(topics),
                    "accent": random.choice(accents)
                })

                current_time += duration + random.uniform(0.3, 1.0)

        print(f"Generated {len(self.conversations)} sample utterances")

    def get_utterances(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get next batch of utterances with timestamp"""
        if not self.conversations:
            return []

        # Get utterances from current position
        end_idx = min(self.current_idx + limit, len(self.conversations))
        utterances = self.conversations[self.current_idx:end_idx]

        # Add current timestamp to each utterance
        current_time = datetime.now(timezone.utc)
        result = []
        for utt in utterances:
            # Create a copy to avoid modifying the original
            utt_copy = utt.copy()
            utt_copy["timestamp"] = current_time
            result.append(utt_copy)

        # Update position and loop if needed
        self.current_idx = end_idx
        if self.current_idx >= len(self.conversations):
            self.current_idx = 0

        return result

    def get_utterances_chunk(self, chunk_size: int = 1000) -> List[Dict[str, Any]]:
        """Get a large chunk of utterances for high-throughput scenarios"""
        return self.get_utterances(limit=chunk_size)


# ============================================================================
# Initialize clients
# ============================================================================

opensky_client = OpenSkyClient()
transcript_client = TranscriptClient()


# ============================================================================
# Root Endpoint
# ============================================================================

@app.get("/", tags=["General"])
async def root():
    """Welcome endpoint with API information"""
    return {
        "message": "OpenSky & Transcript Data API",
        "version": "2.0.0",
        "data_sources": ["opensky", "transcript"],
        "documentation": "/docs",
        "simulation": {
            "target": "500k calls/day",
            "required_throughput": "~78 utterances/second",
            "dataset_size": f"{transcript_client.total_utterances:,} utterances" if hasattr(transcript_client, 'total_utterances') else "unknown"
        },
        "endpoints": {
            "opensky": {
                "status": "/opensky/status",
                "flights": "/opensky/flights",
                "stream": "/opensky/stream"
            },
            "transcript": {
                "status": "/transcript/status",
                "utterances": "/transcript/utterances",
                "stream": "/transcript/stream",
                "high_throughput_stream": "/transcript/stream/high-throughput"
            }
        }
    }


# ============================================================================
# OpenSky Endpoints
# ============================================================================

@app.get("/opensky/status", response_model=DataSourceStatus, tags=["OpenSky"])
async def opensky_status():
    """Get OpenSky data source status"""
    try:
        opensky_client._handle_rate_limit()
        response = opensky_client.session.get(
            "https://opensky-network.org/api/states/all",
            timeout=5
        )
        available = response.status_code == 200
        return DataSourceStatus(
            name="opensky",
            available=available,
            message="OpenSky Network API is available" if available else "OpenSky Network API unavailable",
            records_available=None
        )
    except Exception as e:
        return DataSourceStatus(
            name="opensky",
            available=False,
            message=f"Error: {str(e)}",
            records_available=None
        )


@app.get("/opensky/flights", response_model=List[FlightRecord], tags=["OpenSky"])
async def get_flights(
    region: Optional[Region] = Query(None, description="Geographic region to filter flights"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of flights to return")
):
    """
    Get current flights from OpenSky Network.

    Requires OPENSKY_CLIENT_ID and OPENSKY_CLIENT_SECRET environment variables
    for authenticated access (4000 calls/day vs 100 calls/day anonymous).
    """
    try:
        region_name = region.value if region else "NORTH_AMERICA"
        flights = opensky_client.get_flights(region=region_name)
        return flights[:limit]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/opensky/stream", tags=["OpenSky"])
async def stream_flights(
    region: Optional[Region] = Query(None, description="Geographic region"),
    interval: float = Query(10.0, ge=5.0, le=60.0, description="Seconds between updates (min 5s for rate limiting)"),
    duration: int = Query(60, ge=10, le=600, description="Stream duration in seconds")
):
    """
    Stream flights in real-time (Server-Sent Events).

    Note: OpenSky API requires minimum 5 seconds between requests.
    """
    async def event_generator():
        start_time = datetime.now()
        region_name = region.value if region else "NORTH_AMERICA"

        while (datetime.now() - start_time).total_seconds() < duration:
            try:
                flights = opensky_client.get_flights(region=region_name)

                data = {
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "count": len(flights),
                    "flights": flights
                }

                yield f"data: {json.dumps(data, default=str)}\n\n"

            except Exception as e:
                error_data = {
                    "error": str(e),
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
                yield f"data: {json.dumps(error_data)}\n\n"

            await asyncio.sleep(interval)

        yield f"data: {json.dumps({'status': 'complete'})}\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream"
    )


# ============================================================================
# Transcript Endpoints
# ============================================================================

@app.get("/transcript/status", response_model=DataSourceStatus, tags=["Transcript"])
async def transcript_status():
    """Get Transcript data source status"""
    return DataSourceStatus(
        name="transcript",
        available=True,
        message="Transcript data source is available",
        records_available=len(transcript_client.conversations)
    )


@app.get("/transcript/utterances", response_model=List[TranscriptUtterance], tags=["Transcript"])
async def get_utterances(
    limit: int = Query(10, ge=1, le=100, description="Number of utterances to return")
):
    """
    Get next batch of utterances from transcript dataset.
    """
    try:
        utterances = transcript_client.get_utterances(limit=limit)
        return utterances
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/transcript/stream", tags=["Transcript"])
async def stream_transcripts(
    utterance_delay: float = Query(0.5, ge=0.1, le=5.0, description="Delay between utterances"),
    max_utterances: int = Query(50, ge=5, le=500, description="Total utterances to stream")
):
    """
    Stream transcript utterances in real-time (Server-Sent Events).
    """
    async def event_generator():
        count = 0

        while count < max_utterances:
            utterances = transcript_client.get_utterances(limit=1)

            if utterances:
                data = {
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "utterance": utterances[0],
                    "progress": f"{count + 1}/{max_utterances}"
                }

                yield f"data: {json.dumps(data, default=str)}\n\n"
                count += 1

            await asyncio.sleep(utterance_delay)

        yield f"data: {json.dumps({'status': 'complete', 'total': count})}\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream"
    )


@app.get("/transcript/stream/high-throughput", tags=["Transcript"])
async def stream_transcripts_high_throughput(
    chunk_size: int = Query(100, ge=10, le=10000, description="Utterances per chunk"),
    total_utterances: int = Query(10000, ge=0, le=1000000, description="Total utterances to stream (0 = infinite)"),
    chunk_delay: float = Query(0.01, ge=0.0, le=1.0, description="Delay between chunks (seconds)")
):
    """
    High-throughput transcript streaming for simulating 500k calls/day.

    Streams utterances in large chunks for maximum performance.
    For 500k calls/day with ~13.5 utterances/call = ~78 utterances/second needed.

    Set total_utterances=0 for infinite streaming (until client disconnects).

    Example: chunk_size=100, chunk_delay=0.01 = ~10,000 utterances/second
    """
    async def event_generator():
        sent = 0
        start_time = datetime.now()
        infinite_mode = (total_utterances == 0)

        while infinite_mode or sent < total_utterances:
            # Get next chunk
            current_chunk_size = chunk_size
            if not infinite_mode:
                remaining = total_utterances - sent
                current_chunk_size = min(chunk_size, remaining)

            utterances = transcript_client.get_utterances(limit=current_chunk_size)

            if utterances:
                elapsed = (datetime.now() - start_time).total_seconds()
                current_throughput = sent / elapsed if elapsed > 0 else 0

                data = {
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "chunk_size": len(utterances),
                    "utterances": utterances,
                    "stats": {
                        "total_sent": sent + len(utterances),
                        "elapsed_seconds": round(elapsed, 2),
                        "throughput": round(current_throughput, 2)
                    }
                }

                if not infinite_mode:
                    data["progress"] = {
                        "sent": sent + len(utterances),
                        "total": total_utterances,
                        "percent": round((sent + len(utterances)) / total_utterances * 100, 2)
                    }

                yield f"data: {json.dumps(data, default=str)}\n\n"
                sent += len(utterances)

            if chunk_delay > 0:
                await asyncio.sleep(chunk_delay)

        # Only send completion message if not in infinite mode

        if not infinite_mode:
            elapsed = (datetime.now() - start_time).total_seconds()
            throughput = sent / elapsed if elapsed > 0 else 0

            payload = {
                "status": "complete",
                "total": sent,
                "elapsed_seconds": round(elapsed, 2),
                "throughput": round(throughput, 2),
                "throughput_description": f"{round(throughput, 0)} utterances/second"
            }

            yield "data: " + json.dumps(payload) + "\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream"
    )


@app.get("/transcript/stream/continuous", tags=["Transcript"])
async def stream_transcripts_continuous(
    chunk_size: int = Query(100, ge=10, le=10000, description="Utterances per chunk"),
    chunk_delay: float = Query(0.01, ge=0.0, le=1.0, description="Delay between chunks (seconds)")
):
    """
    TRULY CONTINUOUS transcript streaming - streams forever until client disconnects.

    This endpoint simulates a real-time call center with infinite conversations.
    Perfect for simulating 500k calls/day continuously.

    The stream will never stop - it loops through the dataset indefinitely.
    """
    async def event_generator():
        sent = 0
        start_time = datetime.now()
        chunk_count = 0

        while True:
            utterances = transcript_client.get_utterances(limit=chunk_size)

            if utterances:
                chunk_count += 1
                sent += len(utterances)
                elapsed = (datetime.now() - start_time).total_seconds()
                current_throughput = sent / elapsed if elapsed > 0 else 0

                data = {
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "chunk_number": chunk_count,
                    "chunk_size": len(utterances),
                    "utterances": utterances,
                    "stats": {
                        "total_sent": sent,
                        "elapsed_seconds": round(elapsed, 2),
                        "throughput": round(current_throughput, 2),
                        "throughput_description": f"{round(current_throughput, 0)} utterances/second"
                    }
                }

                yield f"data: {json.dumps(data, default=str)}\n\n"

            if chunk_delay > 0:
                await asyncio.sleep(chunk_delay)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream"
    )


# ============================================================================
# Health Check
# ============================================================================

@app.get("/health", tags=["General"])
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "data_sources": {
            "opensky": "available",
            "transcript": "available"
        }
    }


if __name__ == "__main__":
    import uvicorn
    import os

    # Support Databricks Apps environment
    port = int(os.environ.get("PORT", 8000))
    host = os.environ.get("HOST", "0.0.0.0")

    uvicorn.run(app, host=host, port=port)
