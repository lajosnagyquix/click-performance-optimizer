import os
import random
from quixstreams import Application, State
from quixstreams.models.serializers.quix import QuixDeserializer, QuixTimeseriesSerializer

app = Application.Quix("transformation-v1", auto_offset_reset="latest")

output_topic = app.topic(os.environ["output"], value_serializer=QuixTimeseriesSerializer())

sdf = app.dataframe(output_topic)

def generate_random_event():
    # Generate a random timestamp within the last week
    timestamp = int(random.uniform(time.time() - 7 * 24 * 60 * 60, time.time()) * 1000)

    # Define a set of event types to choose from
    event_types = ["user signup", "purchase", "login"]

    # Generate a random event type
    event_type = random.choice(event_types)

    # Generate a random user ID
    user_id = f"user-{random.randint(1, 100000)}"

    # Generate a random session ID
    session_id = f"session-{random.randint(1, 10000)}"

    # Define a set of device types to choose from
    device_types = ["desktop", "mobile"]

    # Generate a random device type
    device_type = random.choice(device_types)

    # Define a set of operating systems to choose from
    operating_systems = ["Windows", "macOS", "Android", "iOS"]

    # Generate a random operating system
    operating_system = random.choice(operating_systems)

    # Define a set of browser types to choose from
    browser_types = ["Chrome", "Firefox", "Safari"]

    # Generate a random browser type
    browser_type = random.choice(browser_types)

    # Define a set of screen resolutions to choose from
    screen_resolutions = [
        "1920x1080",
        "1280x720",
        "1024x768",
        "800x600",
        "480x320",
    ]

    # Generate a random screen resolution
    screen_resolution = random.choice(screen_resolutions)

    # Define a set of locations to choose from
    locations = [
        "New York",
        "Los Angeles",
        "Chicago",
        "Houston",
        "Phoenix",
    ]

    # Generate a random location
    location = random.choice(locations)

    # Define a set of referrers to choose from
    referrers = [
        "https://www.google.com/",
        "https://www.yahoo.com/",
        "https://www.bing.com/",
    ]

    # Generate a random referrer
    referrer = random.choice(referrers)

    # Define a set of event data fields to choose from
    event_data_fields = [
        "product name",
        "price",
        "quantity",
    ]

    # Generate a random number of event data fields (1-3)
    num_event_data_fields = random.randint(1, 3)

    # Generate random event data fields
    event_data = {
        field: random.choice(["product A", "product B", "product C"])

    # Define a set of user attributes fields to choose from
    user_attributes_fields = [
        "age",
        "gender",
        "interests",
    ]

    # Generate a random number of user attribute fields (1-3)
    num_user_attributes_fields = random.randint(1, 3)

    # Generate random user attributes fields
    user_attributes = {
        field: random.choice(["male", "female"]) if field == "gender" else random.randint(18, 65) if field == "age" else
    random.choice(["music", "movies", "sports"]) for i in range(num_user_attributes_fields)
    }

    # Define a set of event properties fields to choose from
    event_properties_fields = [
        "duration",
        "status code",
    ]

    # Generate a random number of event property fields (1-2)
    num_event_properties_fields = random.randint(1, 2)

    # Generate random event properties fields
    event_properties = {
        field: random.uniform(0, 60) if field == "duration" else random.randint(200, 500) if field == "status code" else None for i in
    range(num_event_properties_fields)
    }

    # Generate a random version number
    version_number = f"v{random.randint(1, 10)}"

    # Define a set of error messages to choose from
    error_messages = [
        "Error 404: Page not found",
        "Internal server error",
    ]

    # Generate a random error message (if any)
    error_message = random.choice(error_messages) if bool(random.randint(0, 1)) else None

    return {
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


# Generate a random event
event = generate_random_event()

print(event)


sdf = sdf.apply(event)

if __name__ == "__main__":
    app.run(sdf)