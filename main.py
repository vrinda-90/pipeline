from apache_beam.options.pipeline_options import PipelineOptions
import os
import google.cloud.pubsub_v1 as pubsub_v1
import logging
import argparse
import apache_beam as beam

stores_schema = 'Store:STRING, Type:STRING, Size:STRING, IngestionTime:STRING'
sales_schema = 'Store:STRING, Dept:STRING, Date:STRING, Weekly_Sales:STRING, IsHoliday:STRING, IngestionTime:STRING'
features_schema = 'Store:STRING, Date:STRING, Temperature:STRING, Fuel_Price:STRING, MarkDown1:STRING, ' \
                  'MarkDown2:STRING, MarkDown3:STRING, MarkDown4:STRING, MarkDown5:STRING, CPI:STRING, ' \
                  'Unemployment:STRING, IsHoliday:STRING, IngestionTime:STRING '

#add project name and credentials json file path here
PROJECT = ''
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = ""


def table_fn(topic):
    if topic == 'stores':
        return '{0}:testdataset.stores_data'.format(PROJECT)
    elif topic == 'features':
        return '{0}:testdataset.features_data'.format(PROJECT)
    elif topic == 'sales':
        return '{0}:testdataset.sales_data'.format(PROJECT)


def table_schema(topic):
    if topic == 'stores':
        return stores_schema
    elif topic == 'features':
        return features_schema
    elif topic == 'sales':
        return sales_schema


class ParseJson(beam.DoFn):

    def process(self, element):
        from datetime import datetime
        import json

        json_array = []
        d = datetime.now()
        date_string = d.strftime("%Y-%m-%d %H:%M:%S")
        dict_obj = json.loads(element)
        dict_obj["IngestionTime"] = date_string
        json_array.append(dict_obj)
        return json_array


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_topic")
    parser.add_argument("--runner")
    known_args = parser.parse_args(argv)

    input_topic = known_args.input_topic
    print(f'argument input in main{input_topic}')
    publisher = pubsub_v1.PublisherClient()
    TOPIC = publisher.topic_path(PROJECT, input_topic)
    print(f'Topic path {TOPIC}')
    schema = input_topic+'_schema'
    print(f'schema name {schema}')

    p = beam.Pipeline(options=PipelineOptions(
        streaming=True,
        runner=known_args.runner,
        project=PROJECT,
        temp_location='gs://retail_sales_0102/temp',
        staging_location='gs://retail_sales_0102/temp',
        region='us-central1',
        num_workers=1,
        max_num_workers=1,
        autoscaling_algorithm=None,
        machine_type='n1-standard-1',
        service_account_email='dataflow-job@herewego-303119.iam.gserviceaccount.com'
    ))

    (p
        | 'ReadData' >> beam.io.ReadFromPubSub(topic=TOPIC).with_output_types(bytes)
        | "Decode" >> beam.Map(lambda x: x.decode('utf-8'))
        | 'ParseJSON' >> beam.ParDo(ParseJson())
        | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(table_fn(input_topic),
                                                       write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                                                       create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                       schema=table_schema(input_topic)
                                                       )
     )
    result = p.run()
    result.wait_until_finish()


if __name__ == '__main__':
    logger = logging.getLogger().setLevel(logging.INFO)
    main()

