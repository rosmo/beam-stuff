import os
import logging
import argparse
import re
from datetime import datetime
import json
import apache_beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.io.textio import ReadFromText, WriteToText, ReadAllFromText
from apache_beam.io.filebasedsource import FileBasedSource
from apache_beam.io.filesystems import FileSystems
import sys

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ReadFiles(apache_beam.DoFn):

    def process(self, element):
        file = { 
            'name': element['name'],
            'content': ReadFromText(element['file']) | apache_beam.combiners.ToList()
        }
        print(file)
        return [file]

class Split(apache_beam.DoFn):

    def process(self, element):
        (name, content) = element

        matches = re.findall(r'\d+', name)
        data = json.loads(content)
        result = []
        for item in data['data']['bikeRentalStations']:
            item['timestamp'] = datetime.fromtimestamp(int(matches[0])).isoformat('T')
            #item['timestamp'] = int(matches[0]
            result.append(item)

        return result

parser = argparse.ArgumentParser()
parser.add_argument("--input", dest="input", required=True)
parser.add_argument("--output", required=True, help=("Output BigQuery table for results specified as: PROJECT:DATASET.TABLE or DATASET.TABLE."))
app_args, pipeline_args = parser.parse_known_args()

input_files = app_args.input
output_filename = 'output.txt'

# project_id = os.environ['DATASTORE_PROJECT_ID']
# credentials_file = os.environ['GOOGLE_APPLICATION_CREDENTIALS']
# client = datastore.Client.from_service_account_json(credentials_file)

options = PipelineOptions()
gcloud_options = options.view_as(GoogleCloudOptions)
# gcloud_options.project = project_id
gcloud_options.job_name = 'test-job'

# Dataflow runner
runner = os.environ['DATAFLOW_RUNNER']
options.view_as(StandardOptions).runner = runner

with apache_beam.Pipeline(options=options) as p:

    inputs = []
    for match in FileSystems.match([input_files]):
        for file in match.metadata_list:
            inputs.append(file.path)

    files = (
        p |
        apache_beam.Create(inputs) 
    )

    read = (
        files |
        ReadAllFromText() |
        apache_beam.Map(lambda x: (os.path.basename(inputs.pop(0)), x))
    )

  
    rows = (
        read |
        apache_beam.ParDo(Split())
    )

    rows | 'Write' >> apache_beam.io.WriteToBigQuery(
        app_args.output,
        schema='stationId:INT64, name:STRING, timestamp:DATETIME, lon:FLOAT64, lat:FLOAT64, spacesAvailable:INT64, bikesAvailable:INT64',
        create_disposition=apache_beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=apache_beam.io.BigQueryDisposition.WRITE_TRUNCATE)
    