"""
API-based Transcript Data Source for Apache Spark - Continuous Streaming

This module provides a custom Spark data source that streams call center transcripts
from a FastAPI backend. It continuously polls the /transcript/utterances endpoint
which loops through the dataset infinitely, simulating real-time call center data.

Features:
- Consumes continuous transcript data from FastAPI backend
- Automatic looping through dataset for infinite streaming
- High throughput capable of handling 500k+ calls/day
- Configurable batch sizes for throughput tuning
- Simple HTTP polling compatible with Spark serialization
- PII-redacted transcripts for privacy compliance

Architecture:
    FastAPI Endpoint (loops infinitely) → Spark Polling → Spark Micro-batches

Usage Example:
    # Basic usage - streams call center transcripts continuously
    df = spark.readStream.format("transcript").load()

    # With API configuration for high throughput
    df = spark.readStream.format("transcript") \\
        .option("api_base_url", "http://localhost:8000") \\
        .option("batch_size", "100") \\
        .option("request_delay", "0.01") \\
        .load()

Schema:
    Each utterance record contains:
    - timestamp: When the utterance occurred
    - conversation_id: Unique conversation identifier
    - utterance_id: Sequential utterance number within conversation
    - speaker: Speaker role (agent/customer)
    - text: The actual utterance text
    - confidence: ASR confidence score
    - start_time: Start time within conversation
    - end_time: End time within conversation
    - domain: Call domain/category
    - topic: Inbound/outbound classification
    - accent: Speaker accent

Author: Databricks Tech Marketing - Example Only
Purpose: Educational Example / Pipeline Simulation
Version: 3.0 - Continuous Streaming
Last Updated: October 2025

================================================================================
LEGAL NOTICES & TERMS OF USE

USAGE RESTRICTIONS:
- Educational and demonstration purposes only
- Must comply with data source API terms of use

DISCLAIMER:
This code is provided "AS IS" for educational purposes only. No warranties,
express or implied. Users assume full responsibility for compliance with all
applicable terms of service and regulations.
================================================================================
"""

import time
import requests
from datetime import datetime, timezone
from typing import Dict, List, Tuple, Iterator

from pyspark.sql.datasource import SimpleDataSourceStreamReader, DataSource
from pyspark.sql.types import *

DS_NAME = "transcript"


class TranscriptStreamReader(SimpleDataSourceStreamReader):
    """
    Stream reader that continuously polls the FastAPI /transcript/utterances endpoint.
    The API automatically loops through the dataset infinitely, providing continuous data.
    """

    DEFAULT_API_URL = "http://localhost:8000"
    DEFAULT_BATCH_SIZE = 100
    DEFAULT_REQUEST_DELAY = 0.05
    MAX_RETRIES = 3

    def __init__(self, schema: StructType, options: Dict[str, str]):
        super().__init__()
        self.schema = schema
        self.options = options

        # API configuration
        self.api_base_url = options.get('api_base_url', self.DEFAULT_API_URL).rstrip('/')
        self.batch_size = int(options.get('batch_size', self.DEFAULT_BATCH_SIZE))
        self.request_delay = float(options.get('request_delay', self.DEFAULT_REQUEST_DELAY))

        # State tracking
        self.total_utterances_sent = 0
        self.start_time = datetime.now(timezone.utc)

        # Test API connectivity
        self._test_api_connection()

    def _test_api_connection(self):
        """Test connectivity to the API"""
        session = requests.Session()
        try:
            url = f"{self.api_base_url}/transcript/status"
            response = session.get(url, timeout=5)
            response.raise_for_status()
            status = response.json()
            print(f"Connected to transcript API: {status.get('message', 'OK')}")
            print(f"Records available: {status.get('records_available', 'unknown')}")
        except Exception as e:
            print(f"Warning: Could not connect to API at {self.api_base_url}: {e}")
            print("Will attempt to fetch data anyway...")
        finally:
            session.close()

    def _fetch_utterances(self, limit: int) -> List[Dict]:
        """Fetch utterances from the API - the API loops through data automatically"""
        url = f"{self.api_base_url}/transcript/utterances"
        params = {'limit': limit}

        for attempt in range(self.MAX_RETRIES):
            try:
                response = requests.get(url, params=params, timeout=10)
                response.raise_for_status()
                utterances = response.json()
                return utterances
            except requests.exceptions.RequestException as e:
                if attempt < self.MAX_RETRIES - 1:
                    print(f"API request failed (attempt {attempt + 1}/{self.MAX_RETRIES}): {e}")
                    time.sleep(1)
                else:
                    print(f"Failed to fetch utterances after {self.MAX_RETRIES} attempts: {e}")
                    return []

        return []

    def initialOffset(self) -> Dict[str, int]:
        """Initialize offset for streaming"""
        return {
            'total_sent': 0
        }

    def read(self, start: Dict[str, int]) -> Tuple[List[Tuple], Dict[str, int]]:
        """
        Read next batch of utterances from the API.
        The API automatically loops through the dataset for continuous streaming.
        """
        # Restore state from offset
        self.total_utterances_sent = start.get('total_sent', 0)

        # Add delay between requests if configured
        if self.request_delay > 0:
            time.sleep(self.request_delay)

        # Fetch utterances from API
        utterances = self._fetch_utterances(self.batch_size)

        if not utterances:
            # Return empty batch but keep offset
            return ([], start)

        # Convert API response to Spark records
        batch = []
        for utt in utterances:
            # Parse timestamp from API response
            timestamp_str = utt.get('timestamp')
            if timestamp_str:
                # Handle ISO format timestamp
                if isinstance(timestamp_str, str):
                    timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                else:
                    timestamp = datetime.now(timezone.utc)
            else:
                timestamp = datetime.now(timezone.utc)

            # Create tuple matching schema
            record = (
                timestamp,
                utt.get('conversation_id', 'unknown'),
                utt.get('utterance_id', 0),
                utt.get('speaker', 'unknown'),
                utt.get('text', ''),
                utt.get('confidence', 1.0),
                utt.get('start_time', 0.0),
                utt.get('end_time', 0.0),
                utt.get('domain'),
                utt.get('topic'),
                utt.get('accent')
            )

            batch.append(record)

        # Update offset
        new_offset = {
            'total_sent': self.total_utterances_sent + len(batch)
        }

        self.total_utterances_sent += len(batch)

        if batch:
            print(f"Fetched batch: {len(batch)} utterances (total: {self.total_utterances_sent})")

        return (batch, new_offset)

    def readBetweenOffsets(self, start: Dict[str, int], end: Dict[str, int]) -> Iterator[Tuple]:
        """Read utterances between offsets"""
        data, _ = self.read(start)
        return iter(data)


class TranscriptDataSource(DataSource):
    """
    Custom Spark Data Source for continuous streaming of call center transcripts.

    This data source polls the FastAPI /transcript/utterances endpoint which
    automatically loops through the dataset infinitely, providing continuous data.
    Perfect for simulating real-time call center data at scale (500k+ calls/day).

    Options:
        - api_base_url: Base URL of the transcript API (default: http://localhost:8000)
        - batch_size: Number of utterances to fetch per request (default: 100)
        - request_delay: Delay between API requests in seconds (default: 0.05)

    Example:
        # Basic streaming
        df = spark.readStream.format("transcript").load()

        # High throughput configuration
        df = spark.readStream.format("transcript") \\
            .option("api_base_url", "http://localhost:8000") \\
            .option("batch_size", "200") \\
            .option("request_delay", "0.01") \\
            .load()
    """

    def __init__(self, options: Dict[str, str] = None):
        super().__init__(options or {})
        self.options = options or {}

    @classmethod
    def name(cls) -> str:
        return DS_NAME

    def schema(self) -> StructType:
        """Define schema for transcript utterances"""
        return StructType([
            StructField("timestamp", TimestampType(), nullable=False),
            StructField("conversation_id", StringType(), nullable=False),
            StructField("utterance_id", IntegerType(), nullable=False),
            StructField("speaker", StringType(), nullable=False),
            StructField("text", StringType(), nullable=False),
            StructField("confidence", DoubleType(), nullable=True),
            StructField("start_time", DoubleType(), nullable=True),
            StructField("end_time", DoubleType(), nullable=True),
            StructField("domain", StringType(), nullable=True),
            StructField("topic", StringType(), nullable=True),
            StructField("accent", StringType(), nullable=True),
        ])

    def simpleStreamReader(self, schema: StructType) -> TranscriptStreamReader:
        """Create stream reader instance"""
        return TranscriptStreamReader(schema, self.options)
