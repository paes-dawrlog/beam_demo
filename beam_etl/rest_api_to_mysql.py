import apache_beam as beam
import requests, json, time
from apache_beam import coders

#DoFnHttpRequest
class HttpRequestFn(beam.DoFn):
   def process(self, input_uri):
       try:
           res = requests.get(input_uri) 
           res.raise_for_status()
       except requests.HTTPError as message:
           logging.error(message)
       yield json.loads(json.dumps(res.text))

with beam.Pipeline() as p:
    ts_load_process = int(time.time())
    data = (
        p
        # To create the PCollection for data analysis
        | beam.Create(['https://random-data-api.com/api/stripe/random_stripe'])
        # Execute the DoFn "HttpRequest" 
        | 'Call API ' >> beam.ParDo(HttpRequestFn())
        | beam.Map(print)   
    )

#pBegin with schema and event timestamp
@beam.ptransform_fn
@beam.typehints.with_input_types(beam.pvalue.PBegin)
@beam.typehints.with_output_types(Dict[str, str])

# pipeline_options = PipelineOptions(pipeline_args)
# pipeline_options.view_as(StandardOptions).streaming = True
# Stripe = NamedTuple(
#      "Stripe",
#      [("id", str),
#          ("uid", str),
#          ("valid_card", int)
#          ("token", str)
#          ("invalid_card", int)
#          ("month", int)
#          ("year", int)
#          ("ccv", int)
#          ("ccv_amex", int)
#      ])
# coders.registry.register_coder(Device, coders.RowCoder)


# results = data.run()

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