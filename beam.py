import apache_beam as beam
import requests
class HttpRequest(beam.DoFn):
#    def __init__(self):
#    def __init__(self, input_header):
    #    self.headers = input_header
   def process(self, input_uri):
       try:
           res = requests.get(input_uri) 
        #    res = requests.get(input_uri, headers=self.headers) 
           res.raise_for_status()
       except requests.HTTPError as message:
           logging.error(message)
       yield res.text


# get_data_pipeline = beam.Pipeline()
# data = 

with beam.Pipeline() as p:

    data = (
        p
        # | beam.Create([REMOTE_URI]) 
        # To create the PCollection for data analysis
        | beam.Create(['https://random-data-api.com/api/stripe/random_stripe?size=30'])
        # Execute the DoFn "HttpRequest" 
        | 'Call API ' >> beam.ParDo(HttpRequest())


# ExampleRow = typing.NamedTuple('ExampleRow',
#                                [('id', int), ('name', unicode)])
# coders.registry.register_coder(ExampleRow, coders.RowCoder)

# with TestPipeline() as p:
#   _ = (
#       p
#       | beam.Create([ExampleRow(1, 'abc')])
#           .with_output_types(ExampleRow)
#       | 'Write to jdbc' >> WriteToJdbc(
#           table_name='jdbc_external_test_write'
#           driver_class_name='org.postgresql.Driver',
#           jdbc_url='jdbc:postgresql://localhost:5432/example',
#           username='postgres',
#           password='postgres',
#       ))