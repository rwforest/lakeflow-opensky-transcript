"""
Test high-throughput transcript streaming
Simulates 500k calls/day workload
"""

import time
from main import transcript_client

def test_throughput(total_utterances=10000, chunk_size=1000):
    """Test how fast we can retrieve utterances"""
    print(f"\n{'='*60}")
    print(f"Testing Throughput")
    print(f"{'='*60}")
    print(f"Total utterances to retrieve: {total_utterances:,}")
    print(f"Chunk size: {chunk_size:,}")
    print(f"Dataset size: {transcript_client.total_utterances:,} utterances\n")

    retrieved = 0
    start_time = time.time()

    while retrieved < total_utterances:
        remaining = total_utterances - retrieved
        current_chunk = min(chunk_size, remaining)

        utterances = transcript_client.get_utterances(limit=current_chunk)
        retrieved += len(utterances)

        if retrieved % 10000 == 0 or retrieved == total_utterances:
            elapsed = time.time() - start_time
            throughput = retrieved / elapsed if elapsed > 0 else 0
            print(f"Retrieved: {retrieved:,}/{total_utterances:,} ({retrieved/total_utterances*100:.1f}%) | "
                  f"Throughput: {throughput:,.0f} utterances/sec")

    elapsed = time.time() - start_time
    throughput = retrieved / elapsed

    print(f"\n{'='*60}")
    print(f"Results:")
    print(f"{'='*60}")
    print(f"Total utterances retrieved: {retrieved:,}")
    print(f"Time elapsed: {elapsed:.2f} seconds")
    print(f"Average throughput: {throughput:,.0f} utterances/second")
    print(f"\nFor 500k calls/day simulation:")
    print(f"  - Required: ~78 utterances/second")
    print(f"  - Achieved: {throughput:,.0f} utterances/second")
    print(f"  - Performance: {throughput/78:.1f}x faster than needed ✓" if throughput > 78 else "  - Performance: Need optimization ✗")
    print(f"{'='*60}\n")

    return throughput

if __name__ == "__main__":
    # Test different scenarios
    print("\n" + "="*60)
    print("HIGH-THROUGHPUT TRANSCRIPT STREAMING TEST")
    print("="*60)

    # Test 1: Small chunks
    print("\nTest 1: Small chunks (100 utterances/chunk)")
    test_throughput(total_utterances=10000, chunk_size=100)

    # Test 2: Medium chunks
    print("\nTest 2: Medium chunks (1000 utterances/chunk)")
    test_throughput(total_utterances=50000, chunk_size=1000)

    # Test 3: Large chunks
    print("\nTest 3: Large chunks (5000 utterances/chunk)")
    test_throughput(total_utterances=100000, chunk_size=5000)

    # Test 4: Simulate one hour of 500k calls/day
    print("\nTest 4: Simulate 1 hour of 500k calls/day")
    one_hour_utterances = int(78 * 3600)  # 78 utterances/sec * 3600 seconds
    print(f"One hour at 78 utterances/sec = {one_hour_utterances:,} utterances")
    test_throughput(total_utterances=one_hour_utterances, chunk_size=1000)
