import os
from quixstreams import Application
from datetime import timedelta

def custom_filter(row):
    rapid_retry_threshold = 3  # Threshold for rapid retries
    multiple_success_threshold = 2  # Threshold for multiple successes

    event_count = row.get('event_count', 0)
    event_type = row.get('event_type', '')

    if event_type in ['click', 'purchase'] and event_count > multiple_success_threshold:
        print(f"Multi-success: Filtering out event_type: {event_type} with event_count: {event_count}")
        return False
    elif event_count > rapid_retry_threshold:
        print(f"Rapid_retry: Filtering out event_type: {event_type} with event_count: {event_count}")
        return False
    return True

app = Application.Quix("transformation-v1", auto_offset_reset="latest")

input_topic = app.topic(os.environ["input"], value_deserializer="json")
output_topic = app.topic(os.environ["output"], value_serializer="json")

sdf = app.dataframe(input_topic)

# Define a tumbling window of 10 seconds
windowed_sdf = sdf.tumbling_window(duration_ms=timedelta(seconds=10), grace_ms=timedelta(seconds=1))

# Aggregate counts by user_id and event_type within each window
aggregated_sdf = windowed_sdf.group_by("user_id", "event_type").count(name="event_count")

# Filter aggregated results using the custom logic
filtered_sdf = aggregated_sdf.filter(custom_filter)

# Output the filtered data to the output topic
filtered_sdf.to_topic(output_topic)

if __name__ == "__main__":
    app.run(filtered_sdf)