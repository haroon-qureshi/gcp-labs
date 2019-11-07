from __future__ import absolute_import
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import bigquery
import re
import logging
import sys
import argparse

PROJECT='haq-labs'
table_desc = 'remote_addr:STRING, timelocal:STRING, request_type:STRING, path:STRING, status:STRING, body_bytes_sent:STRING, http_referer:STRING'

src_path = "./NASA_access_log_95.txt"

def regex_clean(data):
    result = []
    pattern = re.compile(r'^(\S+) (\S+) (\S+) \[([\w:/]+)\] "(\S+) (\S+)\s*(\S+)?\s*" (\d{3}) (\S+)')
    try:
        reg_match = re.match(pattern, data)
        if reg_match:
            for i in range(1,10):
                result.append(reg_match.group(i))
        else:
            result.append(" ")
    except:
        print("There was an error with the regex search")
    #print (result)
    result = [x.strip() for x in result]
    result = [x.replace('"', "") for x in result]
    res = ','.join(result)
    #print(res)
    return res

#class Split(beam.DoFn):

    #def process(self, element):
        #from datetime import datetime
        #element = element.split(",")
        ##d = datetime.strptime(element[3], "%d/%b/%Y:%H:%M:%S")
        ##date_string = d.strftime("%Y-%m-%d %H:%M:%S")
        
        #return [{ 
            #'remote_addr': element[0],
            #'timelocal': element[3],
            #'request_type': element[4],
            #'path': element[5],
            #'body_bytes_sent': element[8],
            #'status': element[7],
            #'http_referer': element[6]
        #}]

class DataIngestion:
    def parse_method(self, string_input):
        # Strip out carriage return, newline and quote characters.
        values = re.split(",",
                          re.sub('\r\n', '', re.sub(u'"', '', string_input)))
        #print(values)
        row = [{ 
            'remote_addr': values[0],
            'timelocal': values[3],
            'request_type': values[4],
            'path': values[5],
            'body_bytes_sent': values[8],
            'status': values[7],
            'http_referer': values[6]
        }]
        #print(row)
        return row
        
def run(argv=None):
    """The main function which creates the pipeline and runs it."""

    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--input',
        dest='input',
        required=False,
        help='Input file to read. This can be a local file or '
        'a file in a Google Storage Bucket.',
        default='gs://haq-labs/NASA_access_log_95.txt')

    parser.add_argument('--output',
                        dest='output',
                        required=False,
                        help='Output BQ table to write results to.',
                        default='haq-labs:demos.logdata')

    # Parse arguments from the command line.
    known_args, pipeline_args = parser.parse_known_args(argv)

    data_ingestion = DataIngestion()

    p = beam.Pipeline(options=PipelineOptions(pipeline_args))

    (p
     | 'Read from a File' >> beam.io.ReadFromText(src_path)
     | 'String To BigQuery Row' >>
     beam.Map(lambda s: data_ingestion.parse_method(s))
     | 'Write to BigQuery' >> beam.io.Write(
         beam.io.BigQuerySink(
             table='logdata',
             dataset='demos',
             project='haq-labs',
             schema=table_desc,
             create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
             write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)))
    p.run().wait_until_finish()

if __name__ == '__main__':
    logger = logging.getLogger()
    run()

    #s = regex_clean('unicomp6.unicomp.net - - [01/Jul/1995:00:00:06] "GET /shuttle/countdown/ HTTP/1.0" 200 3985')
    #data_ingestion = DataIngestion()
    #data_ingestion.parse_method(s)

    #p = beam.Pipeline(options=PipelineOptions())

    #(p
      #| 'ReadData' >> beam.io.textio.ReadFromText(src_path)
      #| "clean address" >> beam.Map(regex_clean)
      #| 'ParseCSV' >> beam.Map(lambda s: data_ingestion.parse_method(s)) #beam.ParDo(Split())
      #| 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
          #table='logdata',
          #dataset='demos',
          #project='haq-labs',
          #schema=table_desc,
          #write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
    #)

    #p.run().wait_until_finish()