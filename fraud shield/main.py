import os
from quixstreams import Application, State
from datetime import timedelta

def process_event_with_window(event, state: State):
    user_id = event["user_id"]
    event_type = event["event_type"]
    current_timestamp = event["timestamp"]  # Ensure 'timestamp' is provided in milliseconds
    
    # Keys for state management
    count_key = f"{user_id}-{event_type}-count"
    start_time_key = f"{user_id}-{event_type}-start"
    
    # Window length in milliseconds (e.g., 10 seconds)
    window_length_ms = 10000
    
    # Retrieve or initialize state
    count = state.get(count_key, default=0)
    start_timestamp = state.get(start_time_key, default=None)
    
    if start_timestamp is None:
        start_timestamp = current_timestamp
        state.set(start_time_key, start_timestamp)
    
    # Check if current event is within the window length from the start
    if current_timestamp - start_timestamp > window_length_ms:
        # Window has expired, reset count and start timestamp
        count = 1  # Reset count for new window
        start_timestamp = current_timestamp  # Reset window start time
        state.set(start_time_key, start_timestamp)  # Update the start timestamp in state
    else:
        # Increment count within the current window
        count += 1
    
    # Update count in state
    state.set(count_key, count)
    
    # Filtering logic based on threshold
    rapid_retry_threshold = 3
    if count > rapid_retry_threshold:
        print(f"Threshold exceeded for {user_id}, {event_type}. Count: {count}")
        event['filter_out'] = True
    else:
        event['filter_out'] = False

    return event

app = Application.Quix("transformation-v1")
input_topic = app.topic(os.environ["input"], value_deserializer="json")
output_topic = app.topic(os.environ["output"], value_serializer="json")

sdf = app.dataframe(input_topic)

# Apply the stateful processing function to each event
sdf_processed = sdf.apply(process_event_with_window, stateful=True)

# Correctly filter and assign the filtered stream for output
sdf_filtered = sdf_processed.filter(lambda event: not event['filter_out'])

# Output the filtered data to the output topic
sdf_filtered = sdf_filtered.to_topic(output_topic)

if __name__ == "__main__":
    app.run(sdf_filtered)  # Make sure to run the filtered stream
