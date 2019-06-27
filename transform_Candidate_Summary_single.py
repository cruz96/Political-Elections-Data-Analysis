import os
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

# DoFn to perform on each element in the input PCollection.
class cleanName(beam.DoFn):
  def process(self, element):
    record = element

    input = record.get('cand_name')

    firstsplit = input.split('/')
    firstsplit = firstsplit[0]
    split = firstsplit.split(' ')
    
        #removes all %.
    badElements = []
    for i in range(len(split)):
        element = split[i]
        if("." in element):
            badElements.append(i)
        #remove II III and IV
        elif(element == "II" or element == "III" or element == "IV" or element == "JR" or element == "SR"):
            badElements.append(i)
        #swap first name with preferred name
        elif('"' in element):
            temp = split[1]
            split[1] = element
            split[i] = temp
    #remove bad elements
    for i in reversed(badElements):
        split.pop(i)
        
    #remove unecessary characters
    for i in range(len(split)):
        split[i] = split[i].replace(',', '')
        split[i] = split[i].replace("'", '')
        split[i] = split[i].replace('"', '')
        #remove name after -
        if("-" in split[i]):
            split[i] = split[i].split("-")[0]

    #if first name is 1 character swap with middle
    if(len(split[1]) == 1):
        temp = split[1]
        split[1] = split[2]
        split[2] = split[1]
        
    #remove elements from end to beginning
    while(len(split)> 2):
        split.pop(-1)
        
    #convert list to string
    output = ""
    for i in reversed(range(len(split))):
        if(i != 0):
            output += split[i] + " "
        else:
            output += split[i]
            
    new_record = {'cand_name': output, 'original': record.get('cand_name')}
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
with beam.Pipeline('DirectRunner', options=opts) as p:

    query_results = p | 'Read from BigQuery' >> beam.io.Read(beam.io.BigQuerySource(query='SELECT cand_name FROM [not-sure-230422.FEC_Information.Candidate_Summary] group by cand_name LIMIT 100'))

    # write PCollection to log file
    query_results | 'Write to log 1' >> WriteToText('query_results.txt')

    # apply a ParDo to the PCollection 
    names_pcoll = query_results | 'Clean Name' >> beam.ParDo(cleanName())

    # write PCollection to log file
    names_pcoll | 'Write to log 2' >> WriteToText('CleanedNames.txt')

    #create table
    #qualified_table_name = PROJECT_ID + ':Election_Outcome_Clean.HOR_and_Senate_Result'
    #table_schema = '{year:YEAR,state:STRING,state_po:STRING,state_fips:INTEGER),state_cen:INTEGER,state_ic:INTEGER,office:STRING,stage:STRING,special:BOOLEAN,candidate:STRING,party:STRING,writein:BOOLEAN,candidatevotes:INTEGER,totalvotes:INTEGER,version:INTEGER}'

    #names_pcoll | 'Write to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(qualified_table_name, 
                                                           # schema=table_schema,  
                                                            #create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                            #write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
