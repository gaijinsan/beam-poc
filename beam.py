import apache_beam as beam
from apache_beam.io.kafka import ReadFromKafka
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam import window
from apache_beam.io import fileio
import datetime
import logging

# Configure logging at the beginning of your script
logging.basicConfig(level=logging.INFO)

# Create a PipelineOptions object
options = PipelineOptions()

# Log the parsed options to confirm the runner is set correctly
logging.info("PipelineOptions: %s", options.get_all_options())

# Define a DoFn for logging elements
class LogElements(beam.DoFn):
    def process(self, element):
        logging.info("Received element from Kafka: %s", element)
        yield element

# Create a unique group ID for this run using the current timestamp
run_id = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
consumer_group_id = f"my-beam-app-test-{run_id}"

consumer_config = {
    "bootstrap.servers": "localhost:9092",
    "group.id": consumer_group_id,
    "auto.offset.reset": "earliest"
}
topics = ["mastodon-posts"]

def create_file_name_based_on_window(window, pane, shard_index, total_shards, compression, destination):
    """Creates a filename including the window's start time and shard info."""
    start_time = window.start.to_rfc3339().replace(':', '-').replace('.', '-')
    return f"output-{start_time}-{shard_index:05d}-of-{total_shards:05d}.txt"

with beam.Pipeline(options=options) as pipeline:
    kafka_records = (
        pipeline
        | ReadFromKafka(
            consumer_config=consumer_config,
            topics=topics,
            max_num_records=40
        )
        | 'Decode Msg' >> beam.Map(lambda kv: kv[1].decode('utf-8'))
        | "Log Kafka Messages" >> beam.ParDo(LogElements())
        | "Apply fixed windows" >> beam.WindowInto(window.FixedWindows(10))
        | "Combine messages" >> beam.Map(lambda x: f"Message: {x}")
        | "Write to Text" >> fileio.WriteToFiles(
            path='output',
            file_naming=create_file_name_based_on_window,
            sink=fileio.TextSink()
        )
    )
