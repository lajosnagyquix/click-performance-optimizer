import os
import random
import time
import threading
import json
import uuid
from quixstreams.kafka import Producer
from quixstreams.models.serializers import (
    QuixTimeseriesSerializer,
    SerializationContext,
)
from quixstreams import Application
from quixstreams.platforms.quix import QuixKafkaConfigsBuilder, TopicCreationConfigs


app = Application.Quix(str(uuid.uuid4()))

output_topic = app.topic(os.environ["output"], value_serializer="json")

def generate_random_choice(choices):
    return random.choice(choices)

def generate_random_event():
    timestamp = int(random.uniform(time.time() - 7 * 24 * 60 * 60, time.time()) * 1000)

    event_types = ["click", "impression", "collapse", "abandonment", "purchase", "error"]
    event_type = generate_random_choice(event_types)

    # how long it took from render to action
    action_time = int(random.uniform(timestamp, time.time()) * 1000)

    campaign_id = f"campaign-{random.randint(1, 20)}"

    creative_alternatives = ["With borders", "jim's idea", "with green borders"]
    creative_alternative = generate_random_choice(creative_alternatives)

    user_id = f"user-{random.randint(1, 100000)}"
    session_id = f"session-{random.randint(1, 10000)}"


    device_types = ["desktop", "mobile", "appliance"]
    device_type = generate_random_choice(device_types)

    operating_systems = ["Windows", "macOS", "Android", "iOS", "embedded"]
    operating_system = generate_random_choice(operating_systems)

    browser_types = ["Chrome", "Firefox", "Safari", "curl"]
    browser_type = generate_random_choice(browser_types)

    browser_fingerprint_hash = str(uuid.uuid8())

    screen_resolutions = ["1920x1080", "1280x720", "1024x768", "800x600", "480x320"]
    screen_resolution = generate_random_choice(screen_resolutions)

    locations = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "EMEA", "APAC", "LATAM"]
    location = generate_random_choice(locations)

    referrers = ["https://www.google.com/", "https://www.yahoo.com/", "https://www.bing.com/"]
    referrer = generate_random_choice(referrers)

    event_data_fields = ["product name", "price", "quantity"]
    event_data = {field: generate_random_choice(["product A", "product B", "product C"]) for field in event_data_fields}

    user_attributes_fields = ["age", "gender", "interests"]
    user_attributes = {
        field: generate_random_choice(["male", "female"]) if field == "gender" else random.randint(18, 65) if field == "age" else generate_random_choice(["music", "movies", "sports"]) for field in user_attributes_fields
    }

    event_properties_fields = ["duration", "status code"]
    event_properties = {
        field: random.uniform(0, 60) if field == "duration" else random.randint(200, 500) for field in event_properties_fields
    }

    creative_iteration = f"v{random.randint(1, 10)}"

    error_code = random.randint(300, 599)
    if event_type != "error":
        error_code = None

    return {
        "timestamp": timestamp,
        "event_type": event_type,
        "campaign_id": campaign_id,
        "creative_alternative": creative_alternative,
        "session_id": session_id,
        "user_id": user_id,
        "device_type": device_type,
        "operating_system": operating_system,
        "browser_type": browser_type,
        "browser_fingerprint_hash": browser_fingerprint_hash,
        "screen_resolution": screen_resolution,
        "location": location,
        "referrer": referrer,
        "event_data": event_data,
        "user_attributes": user_attributes,
        "event_properties": event_properties,
        "creative_iteration": creative_iteration,
        "error_code": error_code,
    }

if __name__ == "__main__":
    def produce_event(event, topic):
        with app.get_producer() as producer:
            producer.produce(
                topic=topic,
                headers=[("uuid", str(uuid.uuid4()))],
                key=event["user_id"],
                value=json.dumps(event),
            )

    while True:
        event = generate_random_event()
        print(event)
        t = threading.Thread(target=produce_event, args=(event, output_topic.name))
        t.start()

        sleep_time = random.uniform(0.2, 5.0)  # Sleep for a random time between 0.2 and 5 seconds
        time.sleep(sleep_time)