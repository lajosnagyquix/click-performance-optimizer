import os
from quixstreams import Application, State

def detect_rapid_retries(row, state: State):
    # Check for rapid retries and update state
    rapid_retry = state.get("user_id") == row["user_id"] and row["timestamp"] - state.get("timestamp", 0) < 10000 and row["event_type"] == state.get("event_type")
    # print to console
    print(f"Rapid retry detected: {rapid_retry} from user {row['user_id']}")
    row["rapid_retry"] = rapid_retry
    # Update state with current row's info
    state.set("user_id", row["user_id"])
    state.set("timestamp", row["timestamp"])
    state.set("event_type", row["event_type"])
    return row

def detect_multiple_success_events(row, state: State):
    # Check for multiple success events and update state
    multiple_success = state.get("user_id") == row["user_id"] and row["event_type"] in ["click", "purchase"]
    # print to console
    print(f"Multiple success events detected: {multiple_success} from user {row['user_id']}")
    row["multiple_success"] = multiple_success
    # Note: No state update needed here for the purpose of this function
    return row

app = Application.Quix("transformation-v1", auto_offset_reset="latest")

input_topic = app.topic(os.environ["input"], value_deserializer="json")
output_topic = app.topic(os.environ["output"], value_serializer="json")

sdf = app.dataframe(input_topic)

# Apply transformations with stateful processing
sdf = sdf.apply(detect_rapid_retries, stateful=True)
sdf = sdf.apply(detect_multiple_success_events, stateful=True)

# Filter out rows where rapid_retry or multiple_success is True
sdf = sdf.filter(lambda row: not row.get("rapid_retry", False))
sdf = sdf.filter(lambda row: not row.get("multiple_success", False))
# produce console output
# Output the filtered sdf to the output topic
sdf = sdf.to_topic(output_topic)

if __name__ == "__main__":
    app.run(sdf)
