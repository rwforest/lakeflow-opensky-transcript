# use OSS pyspark package for declarative pipelines
from pyspark import pipelines as dp

# import and register the datasource
from ..datasources import TranscriptDataSource
spark.dataSource.register(TranscriptDataSource)


# Configuration for transcript streaming
DATASET = "AIxBlock/92k-real-world-call-center-scripts-english"
UTTERANCE_DELAY = 0.1  # seconds between utterances (simulate real-time)
CONVERSATION_DELAY = 2.0  # seconds between conversations
LOOP_CONTINUOUSLY = True  # keep streaming indefinitely
MAX_UTTERANCES = -1  # unlimited, set to positive number to limit


@dp.table
def ingest_transcripts():
    """
    Ingest real-time call center transcripts.

    This transformation streams call center conversations utterance by utterance,
    simulating a real-time transcription pipeline. The data source loads conversations
    from HuggingFace and chunks them into individual utterances.

    Schema:
        - timestamp: When the utterance was ingested
        - conversation_id: Unique conversation identifier
        - utterance_id: Sequential utterance number within conversation
        - speaker: Speaker role (agent/customer)
        - text: The utterance text
        - confidence: ASR confidence score
        - start_time: Start time within conversation (seconds)
        - end_time: End time within conversation (seconds)
        - domain: Call domain/category
        - topic: Inbound/outbound classification
        - accent: Speaker accent (indian/american/filipino)
    """
    return (
        spark.readStream
        .format("transcript")
        .option("dataset", DATASET)
        .option("utterance_delay", str(UTTERANCE_DELAY))
        .option("conversation_delay", str(CONVERSATION_DELAY))
        .option("loop", "true" if LOOP_CONTINUOUSLY else "false")
        .option("max_utterances", str(MAX_UTTERANCES))
        .load()
    )


@dp.table
def transcript_by_speaker():
    """
    Aggregate transcripts by speaker role.

    This shows the distribution of utterances between agents and customers.
    """
    return (
        dp.read("ingest_transcripts")
        .groupBy("speaker", "domain")
        .count()
        .orderBy("domain", "speaker")
    )


@dp.table
def conversation_stats():
    """
    Calculate statistics per conversation.

    Provides insights into conversation length, speaker balance, and confidence scores.
    """
    from pyspark.sql import functions as F

    return (
        dp.read("ingest_transcripts")
        .groupBy("conversation_id", "domain", "topic", "accent")
        .agg(
            F.count("*").alias("total_utterances"),
            F.sum(F.when(F.col("speaker") == "agent", 1).otherwise(0)).alias("agent_utterances"),
            F.sum(F.when(F.col("speaker") == "customer", 1).otherwise(0)).alias("customer_utterances"),
            F.avg("confidence").alias("avg_confidence"),
            F.max("end_time").alias("conversation_duration"),
            F.min("timestamp").alias("conversation_start"),
            F.max("timestamp").alias("conversation_end")
        )
        .orderBy(F.col("conversation_start").desc())
    )
