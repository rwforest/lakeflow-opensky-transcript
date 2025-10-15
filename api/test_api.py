"""
Simple test script to verify the FastAPI endpoints work correctly.

Run after starting the server:
    python test_api.py
"""

import requests
import json
import time
from datetime import datetime

BASE_URL = "http://localhost:8000"


def print_section(title):
    """Print a section header"""
    print("\n" + "=" * 60)
    print(f"  {title}")
    print("=" * 60)


def test_root():
    """Test root endpoint"""
    print_section("Testing Root Endpoint")

    response = requests.get(f"{BASE_URL}/")
    print(f"Status: {response.status_code}")
    print(f"Response: {json.dumps(response.json(), indent=2)}")

    return response.status_code == 200


def test_health():
    """Test health check"""
    print_section("Testing Health Check")

    response = requests.get(f"{BASE_URL}/health")
    print(f"Status: {response.status_code}")
    print(f"Response: {json.dumps(response.json(), indent=2)}")

    return response.status_code == 200


def test_opensky_status():
    """Test OpenSky status"""
    print_section("Testing OpenSky Status")

    response = requests.get(f"{BASE_URL}/opensky/status")
    print(f"Status: {response.status_code}")
    print(f"Response: {json.dumps(response.json(), indent=2)}")

    return response.status_code == 200


def test_opensky_flights():
    """Test OpenSky flights endpoint"""
    print_section("Testing OpenSky Flights")

    response = requests.get(f"{BASE_URL}/opensky/flights", params={"limit": 3})
    print(f"Status: {response.status_code}")

    if response.status_code == 200:
        flights = response.json()
        print(f"Retrieved {len(flights)} flights")

        if flights:
            print(f"\nSample flight:")
            print(json.dumps(flights[0], indent=2))

    return response.status_code == 200


def test_transcript_status():
    """Test Transcript status"""
    print_section("Testing Transcript Status")

    response = requests.get(f"{BASE_URL}/transcript/status")
    print(f"Status: {response.status_code}")
    print(f"Response: {json.dumps(response.json(), indent=2)}")

    return response.status_code == 200


def test_transcript_utterances():
    """Test Transcript utterances endpoint"""
    print_section("Testing Transcript Utterances")

    response = requests.get(f"{BASE_URL}/transcript/utterances", params={"limit": 3})
    print(f"Status: {response.status_code}")

    if response.status_code == 200:
        utterances = response.json()
        print(f"Retrieved {len(utterances)} utterances")

        if utterances:
            print(f"\nSample utterance:")
            print(json.dumps(utterances[0], indent=2))

    return response.status_code == 200


def test_transcript_conversation():
    """Test getting a specific conversation"""
    print_section("Testing Transcript Conversation")

    conversation_id = "conv_00000"
    response = requests.get(f"{BASE_URL}/transcript/conversation/{conversation_id}")
    print(f"Status: {response.status_code}")

    if response.status_code == 200:
        utterances = response.json()
        print(f"Conversation {conversation_id} has {len(utterances)} utterances")

        if utterances:
            print(f"\nFirst utterance:")
            print(f"  Speaker: {utterances[0]['speaker']}")
            print(f"  Text: {utterances[0]['text']}")

    return response.status_code == 200


def test_analytics():
    """Test analytics endpoints"""
    print_section("Testing Analytics Endpoints")

    # Speaker distribution
    response = requests.get(f"{BASE_URL}/transcript/analytics/speaker-distribution")
    print(f"Speaker Distribution Status: {response.status_code}")
    if response.status_code == 200:
        print(f"Response: {json.dumps(response.json(), indent=2)}")

    # Domain distribution
    response = requests.get(f"{BASE_URL}/transcript/analytics/domain-distribution")
    print(f"\nDomain Distribution Status: {response.status_code}")
    if response.status_code == 200:
        print(f"Response: {json.dumps(response.json(), indent=2)}")

    return response.status_code == 200


def test_opensky_stream():
    """Test OpenSky streaming (just check it starts)"""
    print_section("Testing OpenSky Stream (5 seconds)")

    try:
        with requests.get(
            f"{BASE_URL}/opensky/stream",
            params={"duration": 5, "interval": 1},
            stream=True,
            timeout=10
        ) as response:
            print(f"Status: {response.status_code}")

            if response.status_code == 200:
                print("Receiving stream...")
                count = 0

                for line in response.iter_lines():
                    if line and line.startswith(b"data: "):
                        data = json.loads(line[6:])
                        count += 1

                        if "status" in data and data["status"] == "complete":
                            print(f"\nStream completed after {count} events")
                        elif "flights" in data:
                            print(f"  Event {count}: {data['count']} flights at {data['timestamp'][:19]}")

                return True

    except Exception as e:
        print(f"Stream test failed: {e}")
        return False


def test_transcript_stream():
    """Test Transcript streaming (just check it starts)"""
    print_section("Testing Transcript Stream (5 utterances)")

    try:
        with requests.get(
            f"{BASE_URL}/transcript/stream",
            params={"max_utterances": 5, "utterance_delay": 0.2},
            stream=True,
            timeout=10
        ) as response:
            print(f"Status: {response.status_code}")

            if response.status_code == 200:
                print("Receiving stream...")
                count = 0

                for line in response.iter_lines():
                    if line and line.startswith(b"data: "):
                        data = json.loads(line[6:])
                        count += 1

                        if "status" in data and data["status"] == "complete":
                            print(f"\nStream completed: {data['total']} utterances")
                        elif "utterance" in data:
                            utt = data["utterance"]
                            print(f"  {data['progress']}: [{utt['speaker']}] {utt['text'][:50]}...")

                return True

    except Exception as e:
        print(f"Stream test failed: {e}")
        return False


def main():
    """Run all tests"""
    print("\n" + "=" * 60)
    print("  FastAPI Data Sources Testing Suite")
    print("=" * 60)
    print(f"Testing API at: {BASE_URL}")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Check if server is running
    try:
        requests.get(f"{BASE_URL}/health", timeout=2)
    except requests.exceptions.ConnectionError:
        print("\n❌ ERROR: Cannot connect to the API server")
        print("Please start the server first:")
        print("    uvicorn main:app --reload")
        return

    tests = [
        ("Root Endpoint", test_root),
        ("Health Check", test_health),
        ("OpenSky Status", test_opensky_status),
        ("OpenSky Flights", test_opensky_flights),
        ("Transcript Status", test_transcript_status),
        ("Transcript Utterances", test_transcript_utterances),
        ("Transcript Conversation", test_transcript_conversation),
        ("Analytics", test_analytics),
        ("OpenSky Stream", test_opensky_stream),
        ("Transcript Stream", test_transcript_stream),
    ]

    results = []

    for name, test_func in tests:
        try:
            result = test_func()
            results.append((name, result))
        except Exception as e:
            print(f"\n❌ Test failed with exception: {e}")
            results.append((name, False))

        time.sleep(0.5)  # Brief pause between tests

    # Print summary
    print_section("Test Summary")

    passed = sum(1 for _, result in results if result)
    total = len(results)

    for name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status}  {name}")

    print(f"\n{passed}/{total} tests passed")

    if passed == total:
        print("\n🎉 All tests passed!")
    else:
        print(f"\n⚠️  {total - passed} test(s) failed")


if __name__ == "__main__":
    main()
