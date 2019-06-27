import os
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

# DoFn to perform on each element in the input PCollection.
class convertIntToTimestamp(beam.DoFn):
  def process(self, element):
    record = element
    #grab transaction_dt and sub_id
    date = record.get('transaction_dt')
    sub_id = record.get('sub_id')
    #if date is not null grab the first 10 characters
    if(date!=None):
      newDate = date[0:10]
    else:
      #return null if undefined
      newDate = None
    #return newDate and sub_id
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

    query_results = p | 'Read from BigQuery' >> beam.io.Read(beam.io.BigQuerySource(query='SELECT * FROM FEC_Federal_Campaign_Contribution.2012_2014_2016_combined_raw'))

    # write PCollection to log file
    # query_results | 'Write to log 1' >> WriteToText('query_results.txt')

    # apply a ParDo to the PCollection 
    dates_pcoll = query_results | 'Extract Date' >> beam.ParDo(convertIntToTimestamp())

    # write PCollection to log file
    # dates_pcoll | 'Write to log 2' >> WriteToText('transaction_Dates.txt')

    #create table
    qualified_table_name = PROJECT_ID + ':FEC_Federal_Campaign_Contribution.2012_2014_2016_combined_new_dates'
    table_schema = 'transaction_dt:DATE,sub_id:INTEGER'

    dates_pcoll | 'Write to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(qualified_table_name, 
                                                            schema=table_schema,  
                                                            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
