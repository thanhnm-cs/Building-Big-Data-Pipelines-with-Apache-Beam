import json
import apache_beam as beam

from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam import window
from apache_beam.transforms.trigger import (
    AfterProcessingTime,
    AccumulationMode,
    Repeatedly,
    AfterAny,
    AfterCount,
)

import datetime
import pytz


class AddTimestampDoFn(beam.DoFn):
    def process(self, element, **kwargs):
        unix_timestamp = element.get("timestamp")
        if unix_timestamp:
            # 2024-10-06T09:19:16.37383-04:00 and 2024-10-06 09:25:05.273890-04:00 to timestmap
            # "%Y-%m-%d %H:%M:%S.%f%z"
            if "T" in unix_timestamp:
                unix_timestamp = datetime.datetime.strptime(
                    unix_timestamp, "%Y-%m-%dT%H:%M:%S.%f%z"
                )
            else:
                unix_timestamp = datetime.datetime.strptime(
                    unix_timestamp, "%Y-%m-%d %H:%M:%S.%f%z"
                )
            unix_timestamp = unix_timestamp.timestamp()
            yield window.TimestampedValue(element, unix_timestamp)


class ProcessWindow(beam.DoFn):
    def process(self, element, **kwargs):
        print(f"Window contains {len(element)} elements")
        for e in element:
            print(e)


# Define your pipeline options
pipeline_options = PipelineOptions(
    project="ghn-tech",
    region="asia-southeast1",
    runner="DirectRunner",  # Change to 'DataflowRunner' for running on Google Cloud Dataflow
    streaming=True,
)

# Define your Pub/Sub topic
# input_topic = "projects/ghn-tech/subscriptions/taxi-test-sub"
input_topic = "projects/pubsub-public-data/topics/taxirides-realtime"

# Create the pipeline
with beam.Pipeline(options=pipeline_options) as pipeline:
    # Read messages from Pub/Sub
    binary_messages = pipeline | "ReadFromPubSub" >> beam.io.ReadFromPubSub(
        topic=input_topic
    )

    # Process the messages (example: print each message)
    str_messages = binary_messages | "DecodeUtf8" >> beam.Map(
        lambda x: x.decode("utf-8")
    )
    messages = str_messages | "ParseJson" >> beam.Map(json.loads)
    # messages | "PrintMessages" >> beam.Map(print)
    # messages = messages | "AddTimestamp" >> beam.ParDo(AddTimestampDoFn())

    windows = (
        messages
        | "FixedWindows"
        >> beam.WindowInto(
            window.FixedWindows(1 * 60),
            # trigger=Repeatedly(AfterAny(AfterCount(20), AfterProcessingTime(1 * 60))),
            # accumulation_mode=AccumulationMode.DISCARDING,
        )
        | beam.BatchElements(min_batch_size=1, max_batch_size=10000)
    )
    windows | "PrintWindows" >> beam.ParDo(ProcessWindow())
