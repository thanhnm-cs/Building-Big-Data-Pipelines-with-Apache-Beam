import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import pubsub_v1
import time

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
    # Create a Pub/Sub client
    subscriber = pubsub_v1.SubscriberClient()
    topic_path = subscriber.topic_path("pubsub-public-data", "taxirides-realtime")
    subscription_path = subscriber.subscription_path("ghn-tech", "temp-subscription")
    print(topic_path)
    exit()
    # Create a temporary subscription
    subscription = subscriber.create_subscription(
        name=subscription_path, topic=topic_path
    )
    print(f"Created temporary subscription: {subscription.name}")
    # wait for subscription to be ready
    time.sleep(60)
    try:
        # Read messages from the temporary subscription
        messages = pipeline | "ReadFromPubSub" >> beam.io.ReadFromPubSub(
            subscription=subscription_path
        )

        # Process the messages (example: print each message)
        messages | "PrintMessages" >> beam.Map(print)
    finally:
        # Delete the temporary subscription
        subscriber.delete_subscription(subscription=subscription_path)
