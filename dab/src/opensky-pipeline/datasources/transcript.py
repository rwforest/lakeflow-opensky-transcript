"""
HuggingFace Transcript Data Source for Apache Spark - Real-Time Streaming Simulation

This module provides a custom Spark data source that simulates real-time streaming of
call center transcripts from HuggingFace datasets. It loads the AIxBlock 92k call center
dataset and streams utterances one by one to simulate a continuous conversation stream.

Features:
- Loads transcripts from HuggingFace datasets (AIxBlock/92k-real-world-call-center-scripts-english)
- Splits conversations into utterances for realistic streaming
- Simulates real-time streaming with configurable delays
- Supports continuous looping for long-running pipelines
- Word-level timestamps and speaker identification
- PII-redacted transcripts for privacy compliance

Usage Example:
    # Basic usage - streams call center transcripts
    df = spark.readStream.format("transcript").load()

    # With specific dataset and options
    df = spark.readStream.format("transcript") \\
        .option("dataset", "AIxBlock/92k-real-world-call-center-scripts-english") \\
        .option("utterance_delay", "0.5") \\
        .option("loop", "true") \\
        .load()

Schema:
    Each utterance record contains:
    - timestamp: When the utterance occurred
    - conversation_id: Unique conversation identifier
    - utterance_id: Sequential utterance number within conversation
    - speaker: Speaker role (agent/customer)
    - text: The actual utterance text
    - confidence: ASR confidence score
    - domain: Call domain/category
    - topic: Inbound/outbound classification

Author: Databricks Tech Marketing - Example Only
Purpose: Educational Example / Pipeline Simulation
Version: 1.0
Last Updated: October 2025

================================================================================
LEGAL NOTICES & TERMS OF USE

USAGE RESTRICTIONS:
- Educational and demonstration purposes only
- Must comply with HuggingFace dataset terms of use
- Dataset provided by AIxBlock under their license terms

DISCLAIMER:
This code is provided "AS IS" for educational purposes only. No warranties,
express or implied. Users assume full responsibility for compliance with all
applicable terms of service and regulations.
================================================================================
"""

import time
import random
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Tuple, Any, Optional, Iterator
from dataclasses import dataclass
from enum import Enum

from pyspark.sql.datasource import SimpleDataSourceStreamReader, DataSource
from pyspark.sql.types import *

DS_NAME = "transcript"


@dataclass
class Utterance:
    """Represents a single utterance in a conversation"""
    text: str
    speaker: str
    start_time: float
    end_time: float
    confidence: float
    words: List[Dict[str, Any]]


@dataclass
class Conversation:
    """Represents a complete conversation with metadata"""
    conversation_id: str
    utterances: List[Utterance]
    domain: str
    topic: str
    accent: str
    duration: float


class TranscriptStreamReader(SimpleDataSourceStreamReader):
    """
    Stream reader that simulates real-time transcript streaming by:
    1. Loading conversations from HuggingFace
    2. Splitting them into individual utterances
    3. Streaming utterances one by one with realistic timing
    4. Looping continuously to simulate ongoing conversations
    """

    DEFAULT_DATASET = "AIxBlock/92k-real-world-call-center-scripts-english"
    DEFAULT_UTTERANCE_DELAY = 0.1  # seconds between utterances
    DEFAULT_CONVERSATION_DELAY = 1.0  # seconds between conversations
    CHUNK_SIZE = 10  # utterances per batch

    def __init__(self, schema: StructType, options: Dict[str, str]):
        super().__init__()
        self.schema = schema
        self.options = options

        self.dataset_name = options.get('dataset', self.DEFAULT_DATASET)
        self.utterance_delay = float(options.get('utterance_delay', self.DEFAULT_UTTERANCE_DELAY))
        self.conversation_delay = float(options.get('conversation_delay', self.DEFAULT_CONVERSATION_DELAY))
        self.loop = options.get('loop', 'true').lower() == 'true'
        self.max_utterances = int(options.get('max_utterances', '-1'))

        # State tracking
        self.conversations = []
        self.current_conversation_idx = 0
        self.current_utterance_idx = 0
        self.total_utterances_sent = 0
        self.start_time = datetime.now(timezone.utc)

        # Load dataset
        self._load_dataset()

    def _load_dataset(self):
        """
        Load the HuggingFace dataset and parse conversations.
        In a real implementation, this would use the datasets library.
        For simulation, we'll generate realistic sample data.
        """
        try:
            # Try to load from HuggingFace
            try:
                from datasets import load_dataset
                print(f"Loading dataset: {self.dataset_name}")
                dataset = load_dataset(self.dataset_name, split='train', streaming=True)
                self._parse_huggingface_dataset(dataset)
            except ImportError:
                print("HuggingFace datasets library not available, using simulated data")
                self._generate_sample_data()
            except Exception as e:
                print(f"Error loading dataset: {e}, using simulated data")
                self._generate_sample_data()

        except Exception as e:
            print(f"Failed to initialize dataset: {e}")
            self._generate_sample_data()

    def _parse_huggingface_dataset(self, dataset):
        """Parse HuggingFace dataset into Conversation objects"""
        print("Parsing HuggingFace dataset...")

        # Take first 100 conversations for streaming
        for idx, record in enumerate(dataset):
            if idx >= 100:  # Limit initial load
                break

            try:
                # Parse the record structure (adjust based on actual dataset schema)
                conversation_id = record.get('id', f"conv_{idx}")

                # Parse transcript - could be in different formats
                transcript_text = record.get('transcript', record.get('text', ''))

                # Split into utterances (simple split by newlines or speaker markers)
                utterances = self._parse_transcript_text(transcript_text, conversation_id)

                conversation = Conversation(
                    conversation_id=conversation_id,
                    utterances=utterances,
                    domain=record.get('domain', 'unknown'),
                    topic=record.get('topic', 'unknown'),
                    accent=record.get('accent', 'unknown'),
                    duration=record.get('duration', len(utterances) * 5.0)
                )

                self.conversations.append(conversation)

            except Exception as e:
                print(f"Error parsing conversation {idx}: {e}")
                continue

        print(f"Loaded {len(self.conversations)} conversations")

        if len(self.conversations) == 0:
            print("No conversations loaded, falling back to sample data")
            self._generate_sample_data()

    def _parse_transcript_text(self, text: str, conv_id: str) -> List[Utterance]:
        """Parse transcript text into utterances"""
        utterances = []

        # Try to split by common patterns
        lines = text.strip().split('\n')
        current_time = 0.0

        for idx, line in enumerate(lines):
            line = line.strip()
            if not line:
                continue

            # Detect speaker (common patterns: "Agent:", "Customer:", "A:", "C:")
            speaker = "agent"
            cleaned_text = line

            if line.lower().startswith(('agent:', 'a:')):
                speaker = "agent"
                cleaned_text = line.split(':', 1)[1].strip() if ':' in line else line
            elif line.lower().startswith(('customer:', 'c:', 'caller:', 'user:')):
                speaker = "customer"
                cleaned_text = line.split(':', 1)[1].strip() if ':' in line else line

            # Estimate timing
            word_count = len(cleaned_text.split())
            duration = word_count * 0.5  # ~0.5 seconds per word

            utterance = Utterance(
                text=cleaned_text,
                speaker=speaker,
                start_time=current_time,
                end_time=current_time + duration,
                confidence=0.95 + random.random() * 0.05,  # 95-100% confidence
                words=[]  # Could parse word-level timestamps if available
            )

            utterances.append(utterance)
            current_time += duration + 0.5  # Add pause between utterances

        return utterances

    def _generate_sample_data(self):
        """Generate realistic sample call center conversations for simulation"""
        print("Generating sample call center conversations...")

        domains = ['billing', 'technical_support', 'sales', 'customer_service', 'account']
        topics = ['inbound', 'outbound']
        accents = ['indian', 'american', 'filipino']

        # Sample conversation templates
        conversation_templates = [
            [
                ("agent", "Thank you for calling customer support. How may I help you today?"),
                ("customer", "Hi, I'm having trouble with my recent bill. It seems higher than usual."),
                ("agent", "I understand your concern. Let me pull up your account. Can I have your account number please?"),
                ("customer", "Sure, it's account number 12345."),
                ("agent", "Thank you. I can see your account now. Let me review your recent charges."),
                ("agent", "I see the issue. There was a one-time setup fee that was applied this month."),
                ("customer", "Oh, I wasn't aware of that fee. Can you explain what it's for?"),
                ("agent", "Of course. This fee covers the installation of your new service upgrade."),
                ("customer", "I see. That makes sense now. Thank you for clarifying."),
                ("agent", "You're welcome. Is there anything else I can help you with today?"),
                ("customer", "No, that's all. Thank you for your help."),
                ("agent", "Thank you for calling. Have a great day!"),
            ],
            [
                ("agent", "Hello, this is tech support. What seems to be the problem?"),
                ("customer", "My internet connection keeps dropping every few minutes."),
                ("agent", "I'm sorry to hear that. Let's troubleshoot this together."),
                ("agent", "Can you tell me what lights are showing on your modem?"),
                ("customer", "The power light is solid green, but the internet light is blinking red."),
                ("agent", "That indicates a connection issue. Let's try resetting your modem."),
                ("customer", "Okay, I've unplugged it. How long should I wait?"),
                ("agent", "Wait about 30 seconds, then plug it back in."),
                ("customer", "Alright, it's plugged back in now. The lights are coming back on."),
                ("agent", "Great. Let's wait for all the lights to stabilize."),
                ("customer", "Okay, the internet light is now solid green."),
                ("agent", "Perfect! Try accessing a website now."),
                ("customer", "Yes, it's working! Thank you so much!"),
                ("agent", "You're welcome. Call us if you have any more issues."),
            ],
            [
                ("agent", "Good afternoon, I'm calling about our special promotion."),
                ("customer", "What kind of promotion is it?"),
                ("agent", "We're offering 20% off on our premium plan for the next three months."),
                ("customer", "That sounds interesting. What does the premium plan include?"),
                ("agent", "It includes unlimited data, priority support, and access to exclusive features."),
                ("customer", "How much would it cost after the promotional period?"),
                ("agent", "After three months, it would be $49.99 per month."),
                ("customer", "That's a bit more than I'm paying now. Let me think about it."),
                ("agent", "I understand. Would you like me to email you the details?"),
                ("customer", "Yes, that would be helpful."),
                ("agent", "Great. I'll send that over shortly. Thank you for your time."),
            ],
        ]

        # Generate 50 sample conversations
        for i in range(50):
            template = random.choice(conversation_templates)
            conversation_id = f"conv_{i:05d}"

            utterances = []
            current_time = 0.0

            for speaker, text in template:
                word_count = len(text.split())
                duration = word_count * 0.5

                utterance = Utterance(
                    text=text,
                    speaker=speaker,
                    start_time=current_time,
                    end_time=current_time + duration,
                    confidence=0.94 + random.random() * 0.06,
                    words=[]
                )

                utterances.append(utterance)
                current_time += duration + random.uniform(0.3, 1.0)

            conversation = Conversation(
                conversation_id=conversation_id,
                utterances=utterances,
                domain=random.choice(domains),
                topic=random.choice(topics),
                accent=random.choice(accents),
                duration=current_time
            )

            self.conversations.append(conversation)

        print(f"Generated {len(self.conversations)} sample conversations")

    def initialOffset(self) -> Dict[str, int]:
        """Initialize offset for streaming"""
        return {
            'conversation_idx': 0,
            'utterance_idx': 0,
            'total_sent': 0
        }

    def read(self, start: Dict[str, int]) -> Tuple[List[Tuple], Dict[str, int]]:
        """
        Read next batch of utterances.
        This simulates streaming by returning utterances one chunk at a time.
        """
        if not self.conversations:
            print("No conversations available")
            return ([], start)

        # Restore state from offset
        self.current_conversation_idx = start.get('conversation_idx', 0)
        self.current_utterance_idx = start.get('utterance_idx', 0)
        self.total_utterances_sent = start.get('total_sent', 0)

        # Check if we've reached max utterances
        if self.max_utterances > 0 and self.total_utterances_sent >= self.max_utterances:
            if not self.loop:
                print(f"Reached max utterances: {self.max_utterances}")
                return ([], start)

        batch = []
        utterances_collected = 0

        while utterances_collected < self.CHUNK_SIZE:
            # Check if we've exhausted all conversations
            if self.current_conversation_idx >= len(self.conversations):
                if self.loop:
                    print("Looping back to start of conversations...")
                    self.current_conversation_idx = 0
                    self.current_utterance_idx = 0
                    time.sleep(self.conversation_delay)
                else:
                    print("Reached end of conversations")
                    break

            conversation = self.conversations[self.current_conversation_idx]

            # Check if we've exhausted current conversation
            if self.current_utterance_idx >= len(conversation.utterances):
                self.current_conversation_idx += 1
                self.current_utterance_idx = 0
                time.sleep(self.conversation_delay)
                continue

            # Get next utterance
            utterance = conversation.utterances[self.current_utterance_idx]

            # Create timestamp for this utterance (relative to stream start)
            elapsed = (self.total_utterances_sent + utterances_collected) * self.utterance_delay
            timestamp = self.start_time + timedelta(seconds=elapsed)

            # Create tuple matching schema
            record = (
                timestamp,
                conversation.conversation_id,
                self.current_utterance_idx,
                utterance.speaker,
                utterance.text,
                utterance.confidence,
                utterance.start_time,
                utterance.end_time,
                conversation.domain,
                conversation.topic,
                conversation.accent
            )

            batch.append(record)

            # Move to next utterance
            self.current_utterance_idx += 1
            utterances_collected += 1

            # Add realistic delay between utterances
            if self.utterance_delay > 0:
                time.sleep(self.utterance_delay)

        # Update offset
        new_offset = {
            'conversation_idx': self.current_conversation_idx,
            'utterance_idx': self.current_utterance_idx,
            'total_sent': self.total_utterances_sent + utterances_collected
        }

        self.total_utterances_sent += utterances_collected

        if batch:
            print(f"Streaming batch: {len(batch)} utterances (total: {self.total_utterances_sent})")

        return (batch, new_offset)

    def readBetweenOffsets(self, start: Dict[str, int], end: Dict[str, int]) -> Iterator[Tuple]:
        """Read utterances between offsets"""
        data, _ = self.read(start)
        return iter(data)


class TranscriptDataSource(DataSource):
    """
    Custom Spark Data Source for streaming call center transcripts.

    Options:
        - dataset: HuggingFace dataset name (default: AIxBlock/92k-real-world-call-center-scripts-english)
        - utterance_delay: Delay in seconds between utterances (default: 0.1)
        - conversation_delay: Delay in seconds between conversations (default: 1.0)
        - loop: Whether to loop continuously (default: true)
        - max_utterances: Maximum utterances to stream, -1 for unlimited (default: -1)
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
