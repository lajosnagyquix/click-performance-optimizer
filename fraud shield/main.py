import os
from quixstreams import Application, State
from quixstreams.models.serializers.quix import QuixDeserializer, QuixTimeseriesSerializer


def detect_rapid_retries(row, state):
    # if user id is the same as the previous event, and the time difference is less than 10 seconds, then it's a rapid retry
    if state.get("user_id") == row["user_id"] and row["timestamp"] - state.get("timestamp") < 10000:
        row["rapid_retry"] = True
    else:
        row["rapid_retry"] = False

def detect_multiple_success_events(row, state):
    # if user id is the same as one in the trailing window, and the event type is success, then it's a multiple success event, success events are click or purchase
    if state.get("user_id") == row["user_id"] and row["event_type"] in ["click", "purchase"]:
        row["multiple_success"] = True
    else:
        row["multiple_success"] = False

app = Application.Quix("transformation-v1", auto_offset_reset="latest")

input_topic = app.topic(os.environ["input"], value_deserializer="json")
output_topic = app.topic(os.environ["output"], value_serializer="json")

sdf = app.dataframe(input_topic)

# Here put transformation logic.
if "rapid_retry" in sdf.columns:
    # drop the row if it's a rapid retry
    sdf = sdf.filter(lambda row: not row["rapid_retry"])
    # print a message for each rapid retry
    rapid_retries = sdf.filter(lambda row: row["rapid_retry"])

if "multiple_success" in sdf.columns:
    # drop the row if it's a multiple success event
    sdf = sdf.filter(lambda row: not row["multiple_success"])
    # print a message for each multiple success event
    multiple_success = sdf.filter(lambda row: row["multiple_success"])


sdf = sdf.update(lambda row: print(row))

sdf = sdf.to_topic(output_topic)

if __name__ == "__main__":
    app.run(sdf)