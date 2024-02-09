import os
from random import randint, random, choice, uniform
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

consumer_group = app.consumer_group("qlix_raw")

def generate_random_choice(choices):
    return random.choice(choices)

def generate_random_event():
    timestamp = int(random.uniform(time.time() - 7 * 24 * 60 * 60, time.time()) * 1000)

    event_types = ["user signup", "purchase", "login"]
    event_type = generate_random_choice(event_types)

    user_id = f"user-{random.randint(1, 100000)}"
    session_id = f"session-{random.randint(1, 10000)}"

    device_types = ["desktop", "mobile"]
    device_type = generate_random_choice(device_types)

    operating_systems = ["Windows", "macOS", "Android", "iOS"]
    operating_system = generate_random_choice(operating_systems)

    browser_types = ["Chrome", "Firefox", "Safari"]
    browser_type = generate_random_choice(browser_types)

    screen_resolutions = ["1920x1080", "1280x720", "1024x768", "800x600", "480x320"]
    screen_resolution = generate_random_choice(screen_resolutions)

    locations = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"]
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

    version_number = f"v{random.randint(1, 10)}"

    error_messages = ["Error 404: Page not found", "Internal server error"]
    error_message = generate_random_choice(error_messages) if bool(random.randint(0, 1)) else None

    return {
        "timestamp": timestamp,
        "event_type": event_type,  # "user signup", "purchase", "login
        "session_id": session_id,
        "user_id": user_id,
        "device_type": device_type,
        "operating_system": operating_system,
        "browser_type": browser_type,
        "screen_resolution": screen_resolution,
        "location": location,
        "referrer": referrer,
        "event_data": event_data,
        "user_attributes": user_attributes,
        "event_properties": event_properties,
        "version_number": version_number,
        "error_message": error_message,
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
        t = threading.Thread(target=produce_event, args=(event, output_topic))
        t.start()

        sleep_time = random.uniform(0.2, 5.0)  # Sleep for a random time between 0.2 and 5 seconds
        time.sleep(sleep_time)