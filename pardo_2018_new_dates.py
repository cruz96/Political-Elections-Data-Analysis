import os
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

# DoFn to perform on each element in the input PCollection.
class convertIntToTimestamp(beam.DoFn):
  def process(self, element):
    record = element
    #grab sub_id and transaction_dt
    date = record.get('TRANSACTION_DT')
    sub_id = record.get('SUB_ID')
    timestamp = str(date)
    #grab year from last 4 digits
    year = timestamp[-4:]
    #remove last four digits
    timestamp = timestamp[0:-4]
    #grab day from last 2 digits
    day = timestamp[-2:]
    #remove last 2 digits
    timestamp = timestamp[0:-2]
    #if timestamp is not empty get month and format output
    if(timestamp!=""):
      month = int(timestamp)
      newDate = year + "-" + "{:0>2d}".format(month) + "-" + day
    else:
      #else make null
      newDate = None
    #return sub_id and newDate
    new_record = {'transaction_dt': newDate, 'sub_id': sub_id}
    return [new_record]

PROJECT_ID = os.environ['PROJECT_ID']
BUCKET = os.environ['BUCKET']

# Project ID is needed for BigQuery data source, even for local execution.
options = {
    'project': PROJECT_ID,
    'temp_location': BUCKET + '/temp',
    'staging_location': BUCKET + '/staging'
}
opts = beam.pipeline.PipelineOptions(flags=[], **options)

# Create a Pipeline using a local runner for execution.
with beam.Pipeline('DataflowRunner', options=opts) as p:

    query_results = p | 'Read from BigQuery' >> beam.io.Read(beam.io.BigQuerySource(query='SELECT * FROM FEC_Federal_Campaign_Contribution.2018'))

    # write PCollection to log file
    # query_results | 'Write to log 1' >> WriteToText('query_results.txt')

    # apply a ParDo to the PCollection 
    dates_pcoll = query_results | 'Extract Date' >> beam.ParDo(convertIntToTimestamp())

    # write PCollection to log file
    # dates_pcoll | 'Write to log 2' >> WriteToText('transaction_Dates.txt')

    #create table
    qualified_table_name = PROJECT_ID + ':FEC_Federal_Campaign_Contribution.2018_new_dates'
    table_schema = 'transaction_dt:DATE,sub_id:INTEGER'

    dates_pcoll | 'Write to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(qualified_table_name, 
                                                            schema=table_schema,  
                                                            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
