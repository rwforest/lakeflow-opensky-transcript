"""
Generate a large-scale synthetic call center transcript dataset
Simulates 500k calls/day worth of conversations
"""

import json
import random
import os
from pathlib import Path

# Configuration for 500k calls/day simulation
TOTAL_CONVERSATIONS = 92000  # Use full dataset size
UTTERANCES_PER_CONVERSATION_MIN = 8
UTTERANCES_PER_CONVERSATION_MAX = 25
OUTPUT_FILE = "data/conversations.jsonl"

# Conversation templates
TEMPLATES = [
    # Billing conversations
    [
        ("agent", "Thank you for calling billing support. How may I help you today?"),
        ("customer", "Hi, I'm calling about a charge on my recent bill that I don't recognize."),
        ("agent", "I understand your concern. Let me pull up your account. Can I have your account number please?"),
        ("customer", "Sure, it's {account_num}."),
        ("agent", "Thank you. I can see your account now. Let me review your recent charges."),
        ("agent", "I see the charge you're referring to. It's a {charge_type} fee from {date}."),
        ("customer", "Oh, I wasn't aware of that. Can you explain what it's for?"),
        ("agent", "Of course. This fee covers {service_description}."),
        ("customer", "I see. That makes sense now. Thank you for clarifying."),
        ("agent", "You're welcome. Is there anything else I can help you with today?"),
        ("customer", "No, that's all. Thank you for your help."),
        ("agent", "Thank you for calling. Have a great day!"),
    ],

    # Technical support conversations
    [
        ("agent", "Hello, this is tech support. What seems to be the problem?"),
        ("customer", "My {device} keeps {problem} every few minutes."),
        ("agent", "I'm sorry to hear that. Let's troubleshoot this together."),
        ("agent", "Can you tell me what lights are showing on your {device}?"),
        ("customer", "The {indicator1} is solid green, but the {indicator2} is blinking red."),
        ("agent", "That indicates a {issue_type}. Let's try resetting your {device}."),
        ("customer", "Okay, I've unplugged it. How long should I wait?"),
        ("agent", "Wait about 30 seconds, then plug it back in."),
        ("customer", "Alright, it's plugged back in now. The lights are coming back on."),
        ("agent", "Great. Let's wait for all the lights to stabilize."),
        ("customer", "Okay, the {indicator2} is now solid green."),
        ("agent", "Perfect! Try {test_action} now."),
        ("customer", "Yes, it's working! Thank you so much!"),
        ("agent", "You're welcome. Call us if you have any more issues."),
    ],

    # Sales/Outbound conversations
    [
        ("agent", "Good {time_of_day}, I'm calling about our special promotion on {product}."),
        ("customer", "What kind of promotion is it?"),
        ("agent", "We're offering {discount}% off on our {tier} plan for the next {period}."),
        ("customer", "That sounds interesting. What does the {tier} plan include?"),
        ("agent", "It includes {feature1}, {feature2}, and {feature3}."),
        ("customer", "How much would it cost after the promotional period?"),
        ("agent", "After {period}, it would be ${price} per month."),
        ("customer", "That's a bit more than I'm paying now. Let me think about it."),
        ("agent", "I understand. Would you like me to email you the details?"),
        ("customer", "Yes, that would be helpful."),
        ("agent", "Great. I'll send that over shortly. Thank you for your time."),
    ],

    # Customer service - Account inquiry
    [
        ("agent", "Thank you for calling customer service. How can I assist you?"),
        ("customer", "I need to {request_type} on my account."),
        ("agent", "I can help you with that. For security purposes, can you verify your {verification_field}?"),
        ("customer", "{verification_value}."),
        ("agent", "Thank you. I've pulled up your account. Let me {action} for you."),
        ("customer", "How long will this take?"),
        ("agent", "This should be completed within {timeframe}."),
        ("customer", "Okay, and will I receive a confirmation?"),
        ("agent", "Yes, you'll receive a confirmation {notification_method} within {time}."),
        ("customer", "Perfect. Is there anything else I need to do?"),
        ("agent", "No, that's all set. Is there anything else I can help you with?"),
        ("customer", "No, that's everything. Thank you!"),
        ("agent", "You're welcome. Have a wonderful day!"),
    ],

    # Short complaint resolution
    [
        ("agent", "{greeting}, how can I help you today?"),
        ("customer", "I'm very frustrated with {issue}."),
        ("agent", "I sincerely apologize for the inconvenience. Let me see what I can do to resolve this."),
        ("customer", "I've been dealing with this for {duration}."),
        ("agent", "I understand your frustration. I'm going to {resolution_action} right now."),
        ("customer", "When will this be fixed?"),
        ("agent", "I've {action_completed}. It should be resolved within {timeframe}."),
        ("customer", "Okay, I hope so."),
        ("agent", "I'll also {compensation_offer} as an apology for the inconvenience."),
        ("customer", "Thank you, I appreciate that."),
        ("agent", "Is there anything else I can help you with?"),
        ("customer", "No, that's all."),
        ("agent", "Thank you for your patience. Have a great day."),
    ],
]

# Replacement values for templates
REPLACEMENTS = {
    "account_num": lambda: f"{random.randint(10000, 99999)}",
    "charge_type": lambda: random.choice(["service", "activation", "upgrade", "installation", "processing"]),
    "date": lambda: random.choice(["last month", "this month", "two weeks ago", "the 15th"]),
    "service_description": lambda: random.choice([
        "the installation of your new service",
        "the monthly service fee",
        "the equipment rental",
        "the premium feature upgrade"
    ]),
    "device": lambda: random.choice(["modem", "router", "internet connection", "phone", "cable box"]),
    "problem": lambda: random.choice(["disconnecting", "freezing", "not working", "showing errors", "running slow"]),
    "indicator1": lambda: random.choice(["power light", "connection light", "status LED"]),
    "indicator2": lambda: random.choice(["internet light", "DSL light", "online indicator", "WAN light"]),
    "issue_type": lambda: random.choice(["connection issue", "network problem", "configuration error", "signal problem"]),
    "test_action": lambda: random.choice(["accessing a website", "making a call", "checking your service"]),
    "time_of_day": lambda: random.choice(["morning", "afternoon", "evening"]),
    "product": lambda: random.choice(["internet service", "mobile plans", "cable packages", "bundle deals"]),
    "discount": lambda: str(random.choice([10, 15, 20, 25, 30])),
    "tier": lambda: random.choice(["premium", "basic", "professional", "family", "business"]),
    "period": lambda: random.choice(["three months", "six months", "the first year"]),
    "feature1": lambda: random.choice(["unlimited data", "24/7 support", "free installation"]),
    "feature2": lambda: random.choice(["priority customer service", "no contracts", "free equipment"]),
    "feature3": lambda: random.choice(["exclusive features", "cloud storage", "streaming services"]),
    "price": lambda: f"{random.randint(29, 99)}.99",
    "request_type": lambda: random.choice(["update my address", "change my payment method", "upgrade my plan", "cancel a service"]),
    "verification_field": lambda: random.choice(["date of birth", "last four digits of your social", "account PIN"]),
    "verification_value": lambda: random.choice(["January 15th", "5432", "1234"]),
    "action": lambda: random.choice(["process that change", "update that information", "make that modification"]),
    "timeframe": lambda: random.choice(["24-48 hours", "1-2 business days", "by end of day", "within the hour"]),
    "notification_method": lambda: random.choice(["via email", "via text message", "in your online account"]),
    "time": lambda: random.choice(["an hour", "24 hours", "the next business day"]),
    "greeting": lambda: random.choice([
        "Thank you for calling",
        "Hello, thank you for contacting us",
        "Good day, thanks for calling"
    ]),
    "issue": lambda: random.choice(["the service quality", "my recent experience", "this billing error", "the lack of response"]),
    "duration": lambda: random.choice(["a week", "two weeks", "over a month", "several days"]),
    "resolution_action": lambda: random.choice([
        "escalate this to a supervisor",
        "process a refund",
        "upgrade your service at no charge",
        "apply a credit to your account"
    ]),
    "action_completed": lambda: random.choice([
        "processed the request",
        "escalated your case",
        "applied the credit",
        "scheduled a technician"
    ]),
    "compensation_offer": lambda: random.choice([
        "credit one month of service",
        "waive the service fee",
        "provide a discount on your next bill",
        "upgrade you at no extra charge"
    ]),
}

def fill_template(template_text):
    """Fill in template placeholders with random values"""
    result = template_text
    for key, value_func in REPLACEMENTS.items():
        placeholder = "{" + key + "}"
        if placeholder in result:
            result = result.replace(placeholder, value_func())
    return result

def generate_conversation(conv_id):
    """Generate a single conversation"""
    template = random.choice(TEMPLATES)

    # Add some variation by randomly repeating or removing utterances
    if random.random() < 0.3:  # 30% chance to extend conversation
        # Duplicate some utterances with variations
        extended_template = list(template)
        num_extensions = random.randint(1, 5)
        for _ in range(num_extensions):
            idx = random.randint(1, len(template) - 2)
            speaker = template[idx][0]
            if speaker == "customer":
                extended_template.insert(idx + 1, (speaker, random.choice([
                    "I see.",
                    "Okay, got it.",
                    "Understood.",
                    "That makes sense.",
                    "Alright."
                ])))
            else:
                extended_template.insert(idx + 1, (speaker, random.choice([
                    "Let me check on that for you.",
                    "One moment please.",
                    "I'm looking into that now.",
                    "Give me just a second.",
                ])))
        template = extended_template

    utterances = []
    current_time = 0.0

    domains = ['billing', 'technical_support', 'sales', 'customer_service']
    topics = ['inbound', 'outbound']
    accents = ['american', 'british', 'indian', 'filipino', 'australian']

    for idx, (speaker, text_template) in enumerate(template):
        text = fill_template(text_template)
        word_count = len(text.split())
        duration = word_count * random.uniform(0.4, 0.6)  # Vary speaking speed

        utterances.append({
            "conversation_id": conv_id,
            "utterance_id": idx,
            "speaker": speaker,
            "text": text,
            "confidence": round(0.92 + random.random() * 0.08, 4),  # 92-100% confidence
            "start_time": round(current_time, 2),
            "end_time": round(current_time + duration, 2),
            "domain": random.choice(domains),
            "topic": random.choice(topics),
            "accent": random.choice(accents),
        })

        # Add natural pause between utterances
        current_time += duration + random.uniform(0.2, 1.5)

    return utterances

def generate_dataset():
    """Generate the full dataset"""
    print(f"Generating {TOTAL_CONVERSATIONS:,} conversations...")

    # Create output directory
    Path("data").mkdir(exist_ok=True)

    total_utterances = 0

    with open(OUTPUT_FILE, 'w') as f:
        for i in range(TOTAL_CONVERSATIONS):
            conv_id = f"conv_{i:06d}"
            utterances = generate_conversation(conv_id)

            # Write each utterance as a JSON line
            for utterance in utterances:
                f.write(json.dumps(utterance) + '\n')
                total_utterances += 1

            if (i + 1) % 1000 == 0:
                print(f"Generated {i + 1:,} conversations ({total_utterances:,} utterances)")

    avg_utterances = total_utterances / TOTAL_CONVERSATIONS
    print(f"\nDataset generation complete!")
    print(f"Total conversations: {TOTAL_CONVERSATIONS:,}")
    print(f"Total utterances: {total_utterances:,}")
    print(f"Average utterances per conversation: {avg_utterances:.1f}")
    print(f"Output file: {OUTPUT_FILE}")
    print(f"\nFor 500k calls/day simulation:")
    print(f"  Estimated daily utterances: {int(500000 * avg_utterances):,}")
    print(f"  Required throughput: {int(500000 * avg_utterances / 86400)} utterances/second")

if __name__ == "__main__":
    generate_dataset()
