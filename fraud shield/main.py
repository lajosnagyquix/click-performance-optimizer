import os
from quixstreams import Application, State
from datetime import timedelta

def process_event_with_window(event, state: State):
    user_id = event["user_id"]
    event_type = event["event_type"]
    current_timestamp = event["timestamp"]  # Assuming 'timestamp' is in milliseconds
    
    # Keys for state management
    count_key = f"{user_id}-{event_type}-count"
    start_time_key = f"{user_id}-{event_type}-start"
    
    # Window length in milliseconds (e.g., 10 seconds)
    window_length_ms = 10000
    
    # Retrieve or initialize state
    count = state.get(count_key, default=0)
    start_timestamp = state.get(start_time_key, default=current_timestamp)
    
    # Check if current event is within the window length from the start
    if current_timestamp - start_timestamp > window_length_ms:
        # Window has expired, reset count and start timestamp
        count = 1  # Reset count for new window
        start_timestamp = current_timestamp  # Reset window start time
    else:
        # Increment count within the current window
        count += 1
    
    # Update state
    state.set(count_key, count)
    state.set(start_time_key, start_timestamp)
    
    # Define threshold for action (e.g., filtering)
    rapid_retry_threshold = 3
    
    # Filtering logic based on threshold
    if count > rapid_retry_threshold:
        print(f"Threshold exceeded for {user_id}, {event_type}. Count: {count}")
        event['filter_out'] = True  # Flag this event to be filtered out
    else:
        event['filter_out'] = False  # This event does not exceed the threshold

    return event  # Return the modified event

# Apply this function similarly to the previous example


app = Application.Quix("transformation-v1")
input_topic = app.topic(os.environ["input"], value_deserializer="json")
output_topic = app.topic(os.environ["output"], value_serializer="json")

sdf = app.dataframe(input_topic)

# Apply the stateful processing function to each event

sdf_processed = sdf.apply(process_event_with_window, stateful=True)

sdf_filtered = sdf_processed.filter(lambda event: not event['filter_out'])


# Output the processed data to the output topic
# This step would include only events that passed the filtering logic
sdf_processed = sdf_processed.to_topic(output_topic)

if __name__ == "__main__":
    app.run(sdf_processed)
