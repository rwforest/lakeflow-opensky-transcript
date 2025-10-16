"""
FastAPI Application for Testing Custom Spark Data Sources

This application provides REST endpoints to test the OpenSky and Transcript
data sources without needing a full Spark environment. It simulates the
data source behavior and returns sample data.

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

# Import our data source readers (simplified versions for testing)
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../dab/src/opensky-pipeline'))

app = FastAPI(
    title="Spark Data Sources Testing API",
    description="Test OpenSky and Transcript data sources via REST endpoints",
    version="1.0.0"
)

# Configure CORS to allow requests from the demo page
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins for development
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
    longitude: float
    latitude: float
    geo_altitude: Optional[float]
    on_ground: bool
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
    domain: str
    topic: str
    accent: str


class DataSourceStatus(BaseModel):
    """Status of a data source"""
    name: str
    available: bool
    message: str
    records_available: int


# ============================================================================
# OpenSky Data Source Simulation
# ============================================================================

class OpenSkySimulator:
    """Simulates OpenSky data source for testing"""

    def __init__(self):
        self.sample_flights = self._generate_sample_flights()

    def _generate_sample_flights(self) -> List[Dict[str, Any]]:
        """Generate sample flight data"""
        import random

        flights = []
        callsigns = ["UAL123", "DAL456", "AAL789", "SWA101", "JBU202",
                     "AFR303", "BAW404", "DLH505", "ACA606", "ANA707"]
        countries = ["United States", "United Kingdom", "Germany", "France",
                    "Canada", "Japan", "Australia", "Brazil"]

        for i in range(50):
            flights.append({
                "icao24": f"{hex(random.randint(0, 16777215))[2:]:0>6}",
                "callsign": random.choice(callsigns),
                "origin_country": random.choice(countries),
                "longitude": random.uniform(-180, 180),
                "latitude": random.uniform(-90, 90),
                "geo_altitude": random.uniform(0, 12000),
                "on_ground": random.choice([True, False]),
                "velocity": random.uniform(0, 250),
                "true_track": random.uniform(0, 360),
                "vertical_rate": random.uniform(-10, 10)
            })

        return flights

    def get_flights(self, region: Optional[Region] = None, limit: int = 10) -> List[Dict[str, Any]]:
        """Get sample flights"""
        flights = self.sample_flights[:limit]

        # Add timestamp
        for flight in flights:
            flight["time_ingest"] = datetime.now(timezone.utc).isoformat()

        return flights


# ============================================================================
# Transcript Data Source Simulation
# ============================================================================

class TranscriptSimulator:
    """Simulates Transcript data source for testing"""

    def __init__(self):
        self.conversations = self._generate_conversations()
        self.current_idx = 0

    def _generate_conversations(self) -> List[Dict[str, Any]]:
        """Generate sample conversations"""
        import random

        templates = [
            [
                ("agent", "Thank you for calling customer support. How may I help you today?"),
                ("customer", "Hi, I'm having trouble with my recent bill."),
                ("agent", "I understand your concern. Let me pull up your account."),
                ("customer", "Sure, my account number is 12345."),
                ("agent", "Thank you. I can see your account now."),
                ("customer", "Great, what do you see?"),
                ("agent", "I see the issue. There was a one-time setup fee."),
                ("customer", "Oh, I wasn't aware of that fee."),
                ("agent", "It covers the installation of your new service."),
                ("customer", "That makes sense now. Thank you."),
            ],
            [
                ("agent", "Hello, this is tech support. What's the problem?"),
                ("customer", "My internet keeps dropping every few minutes."),
                ("agent", "I'm sorry to hear that. Let's troubleshoot this."),
                ("customer", "Okay, what should I do?"),
                ("agent", "Can you check the lights on your modem?"),
                ("customer", "The internet light is blinking red."),
                ("agent", "Let's try resetting your modem."),
                ("customer", "Okay, I've unplugged it."),
                ("agent", "Wait 30 seconds then plug it back in."),
                ("customer", "Done. The lights are coming back on."),
                ("agent", "Great! Try accessing a website now."),
                ("customer", "It's working! Thank you!"),
            ],
        ]

        conversations = []
        domains = ['billing', 'technical_support', 'sales', 'customer_service']
        topics = ['inbound', 'outbound']
        accents = ['american', 'indian', 'filipino']

        for i in range(20):
            template = random.choice(templates)
            conv_id = f"conv_{i:05d}"

            utterances = []
            current_time = 0.0

            for utt_id, (speaker, text) in enumerate(template):
                duration = len(text.split()) * 0.5

                utterances.append({
                    "conversation_id": conv_id,
                    "utterance_id": utt_id,
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

            conversations.extend(utterances)

        return conversations

    def get_utterances(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get next batch of utterances"""
        utterances = self.conversations[self.current_idx:self.current_idx + limit]

        # Add timestamp
        for utt in utterances:
            utt["timestamp"] = datetime.now(timezone.utc).isoformat()

        self.current_idx = (self.current_idx + limit) % len(self.conversations)

        return utterances

    def get_conversation(self, conversation_id: str) -> List[Dict[str, Any]]:
        """Get all utterances for a specific conversation"""
        utterances = [u for u in self.conversations if u["conversation_id"] == conversation_id]

        for utt in utterances:
            utt["timestamp"] = datetime.now(timezone.utc).isoformat()

        return utterances


# ============================================================================
# Initialize simulators
# ============================================================================

opensky_sim = OpenSkySimulator()
transcript_sim = TranscriptSimulator()


# ============================================================================
# Root Endpoint
# ============================================================================

@app.get("/", tags=["General"])
async def root():
    """Welcome endpoint with API information"""
    return {
        "message": "Spark Data Sources Testing API",
        "version": "1.0.0",
        "data_sources": ["opensky", "transcript"],
        "documentation": "/docs",
        "endpoints": {
            "opensky": {
                "status": "/opensky/status",
                "flights": "/opensky/flights",
                "stream": "/opensky/stream"
            },
            "transcript": {
                "status": "/transcript/status",
                "utterances": "/transcript/utterances",
                "conversation": "/transcript/conversation/{id}",
                "stream": "/transcript/stream"
            }
        }
    }


# ============================================================================
# OpenSky Endpoints
# ============================================================================

@app.get("/opensky/status", response_model=DataSourceStatus, tags=["OpenSky"])
async def opensky_status():
    """Get OpenSky data source status"""
    return DataSourceStatus(
        name="opensky",
        available=True,
        message="OpenSky data source is available (simulated)",
        records_available=len(opensky_sim.sample_flights)
    )


@app.get("/opensky/flights", response_model=List[FlightRecord], tags=["OpenSky"])
async def get_flights(
    region: Optional[Region] = Query(None, description="Geographic region to filter flights"),
    limit: int = Query(10, ge=1, le=100, description="Maximum number of flights to return")
):
    """
    Get current flights from OpenSky data source.

    This endpoint simulates reading from the OpenSky Network API.
    """
    try:
        flights = opensky_sim.get_flights(region=region, limit=limit)
        return flights
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/opensky/stream", tags=["OpenSky"])
async def stream_flights(
    region: Optional[Region] = Query(None, description="Geographic region"),
    interval: float = Query(1.0, ge=0.1, le=10.0, description="Seconds between updates"),
    duration: int = Query(30, ge=5, le=300, description="Stream duration in seconds")
):
    """
    Stream flights in real-time (Server-Sent Events).

    Returns a continuous stream of flight data for the specified duration.
    """
    async def event_generator():
        start_time = datetime.now()

        while (datetime.now() - start_time).total_seconds() < duration:
            flights = opensky_sim.get_flights(region=region, limit=5)

            data = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "count": len(flights),
                "flights": flights
            }

            yield f"data: {json.dumps(data)}\n\n"

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
        message="Transcript data source is available (simulated)",
        records_available=len(transcript_sim.conversations)
    )


@app.get("/transcript/utterances", response_model=List[TranscriptUtterance], tags=["Transcript"])
async def get_utterances(
    limit: int = Query(10, ge=1, le=100, description="Number of utterances to return")
):
    """
    Get next batch of utterances from transcript stream.

    This simulates the streaming transcript data source.
    """
    try:
        utterances = transcript_sim.get_utterances(limit=limit)
        return utterances
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/transcript/conversation/{conversation_id}", response_model=List[TranscriptUtterance], tags=["Transcript"])
async def get_conversation(conversation_id: str):
    """
    Get all utterances for a specific conversation.
    """
    utterances = transcript_sim.get_conversation(conversation_id)

    if not utterances:
        raise HTTPException(status_code=404, detail=f"Conversation {conversation_id} not found")

    return utterances


@app.get("/transcript/stream", tags=["Transcript"])
async def stream_transcripts(
    utterance_delay: float = Query(0.5, ge=0.1, le=5.0, description="Delay between utterances"),
    max_utterances: int = Query(50, ge=5, le=500, description="Total utterances to stream")
):
    """
    Stream transcript utterances in real-time (Server-Sent Events).

    Simulates real-time conversation streaming utterance by utterance.
    """
    async def event_generator():
        count = 0

        while count < max_utterances:
            utterances = transcript_sim.get_utterances(limit=1)

            if utterances:
                data = {
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "utterance": utterances[0],
                    "progress": f"{count + 1}/{max_utterances}"
                }

                yield f"data: {json.dumps(data)}\n\n"
                count += 1

            await asyncio.sleep(utterance_delay)

        yield f"data: {json.dumps({'status': 'complete', 'total': count})}\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream"
    )


# ============================================================================
# Analytics Endpoints
# ============================================================================

@app.get("/transcript/analytics/speaker-distribution", tags=["Analytics"])
async def speaker_distribution():
    """Get distribution of utterances by speaker"""
    agent_count = sum(1 for u in transcript_sim.conversations if u["speaker"] == "agent")
    customer_count = sum(1 for u in transcript_sim.conversations if u["speaker"] == "customer")

    return {
        "total_utterances": len(transcript_sim.conversations),
        "agent_utterances": agent_count,
        "customer_utterances": customer_count,
        "agent_percentage": round(agent_count / len(transcript_sim.conversations) * 100, 2),
        "customer_percentage": round(customer_count / len(transcript_sim.conversations) * 100, 2)
    }


@app.get("/transcript/analytics/domain-distribution", tags=["Analytics"])
async def domain_distribution():
    """Get distribution of conversations by domain"""
    from collections import Counter

    domains = [u["domain"] for u in transcript_sim.conversations]
    distribution = Counter(domains)

    return {
        "total_utterances": len(transcript_sim.conversations),
        "domains": dict(distribution)
    }


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
    uvicorn.run(app, host="0.0.0.0", port=8000)
