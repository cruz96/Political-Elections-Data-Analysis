import os
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

# DoFn to perform on each element in the input PCollection.
class cleanName(beam.DoFn):
  def process(self, element):
    record = element

    input = record.get('candidate').upper()

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
    if(len(split) > 2):
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
    
    

    year = str(record.get('year')) + '-01-01'
            
    if(output != "" and output != "BLANK" and output!= "VOTE BLANK" and output!="VOTE VOID" and output!="SCATTERING" and output !="VOTE OVER" and output != "VOTE/SCATTERING BLANK" and output != "OTHER"):
        new_record = {'year':year, 'state':record.get('state'), 'state_po':record.get('state_po'), 'state_fips':record.get('state_fips'), 'state_cen':record.get('state_cen'), 'state_ic':record.get('state_ic'), 'office':record.get('office'), 'candidate': output, 'party':record.get('party'), 'writein':record.get('writein'), 'candidatevotes':record.get('candidatevotes'), 'totalvotes':record.get('totalvotes'), 'version':record.get('version')}
    
        return [new_record]
    else:
        return

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

    query_results = p | 'Read from BigQuery' >> beam.io.Read(beam.io.BigQuerySource(query='SELECT year, state, state_po, state_fips, state_cen, state_ic, office, candidate, party, writein, candidatevotes, totalvotes, version FROM [not-sure-230422:Election_Outcome.President_Result_By_State] where year >= 2012'))

    # write PCollection to log file
    #query_results | 'Write to log 1' >> WriteToText('query_results.txt')

    # apply a ParDo to the PCollection 
    names_pcoll = query_results | 'Clean Name' >> beam.ParDo(cleanName())

    # write PCollection to log file
    #names_pcoll | 'Write to log 2' >> WriteToText('CleanedNames.txt')

    #create table
    qualified_table_name = PROJECT_ID + ':Election_Outcome_Clean.President_By_State_Result'
    table_schema = 'year:DATE,state:STRING,state_po:STRING,state_fips:INTEGER,state_cen:INTEGER,state_ic:INTEGER,office:STRING,candidate:STRING,party:STRING,writein:BOOLEAN,candidatevotes:INTEGER,totalvotes:INTEGER,version:INTEGER'

    names_pcoll | 'Write to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(qualified_table_name, 
                                                            schema=table_schema,  
                                                            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
