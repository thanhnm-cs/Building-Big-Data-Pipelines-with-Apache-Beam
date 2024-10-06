import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

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

# projects/pubsub-public-data/topics/taxirides-realtime
# Create the pipeline
with beam.Pipeline(options=pipeline_options) as pipeline:
    # Read messages from Pub/Sub
    messages = pipeline | "ReadFromPubSub" >> beam.io.ReadFromPubSub(topic=input_topic)

    # Process the messages (example: print each message)
    messages | "PrintMessages" >> beam.Map(print)
