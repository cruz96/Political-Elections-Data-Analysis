import os
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

# DoFn to perform on each element in the input PCollection.
class cleanName(beam.DoFn):
  def process(self, element):
    record = element
    #grab transaction_dt and sub_id
    input = record.get('candidate')

    input = input.upper()
    split = input.split(' ')
    
    #removes all %.
    badElements = []
    for i in range(len(split)):
        element = split[i]
        if("." in element):
            badElements.append(i)
        #remove II III and IV
        elif(element == "II" or element == "III" or element == "IV" or element == "JR" or element == "SR"):
            badElements.append(i)
    #remove bad elements
    for i in reversed(badElements):
        split.pop(i)
        
    for i in range(len(split)):
        if('"' in split[i]):
            temp = split[0]
            split[0] = split[i].replace('"', '')
            split[i] = temp
    
    #removes " element if more than 3 elements
    badElements = []
    if(len(split) == 2):
        #removes "
        for i in range(len(split)):
            split[i] = split[i].replace('"', '')
            split[i] = split[i].replace('(', '')
            split[i] = split[i].replace(')', '')
            split[i] = split[i].replace('?', '')
    elif(len(split) == 1):
        split = [""]
    else:
        for i in range(len(split)):
            element = split[i]
            if(')' in element or '(' in element): #'"' in element or 
                badElements.append(i)
        for i in reversed(badElements):
            split.pop(i)
    while(len(split) >= 3):
        split.pop(1)
    
    #remove all other characters
    for i in range(len(split)):
        split[i] = split[i].replace(',', '')
        split[i] = split[i].replace("'", '')
        #remove name after -
        if("-" in split[i]):
            split[i] = split[i].split("-")[0]
            
    #convert list to string
    output = ""
    for i in range(len(split)):
        if(i != len(split)-1):
            output += split[i] + " "
        else:
            output += split[i]

    year = str(record.get('year')) + '-01-01'
            
    if(output != "" and output != "BLANK" and output!= "BLANK VOTE" and output!="VOID VOTE" and output!="SCATTERING" and output !="OVER VOTE" and output != "BLANK VOTE/SCATTERING"):
        new_record = {'year':year, 'state':record.get('state'), 'state_po':record.get('state_po'), 'county':record.get('county'), 'FIPS':record.get('FIPS'), 'office':record.get('office'), 'candidate': output, 'party':record.get('party'), 'candidatevotes':record.get('candidatevotes'), 'totalvotes':record.get('totalvotes'), 'version':record.get('version')}
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

    query_results = p | 'Read from BigQuery' >> beam.io.Read(beam.io.BigQuerySource(query='SELECT year, state, state_po, county, FIPS, office, candidate, party, candidatevotes, totalvotes, version FROM [not-sure-230422:Election_Outcome.President_Result_By_County] where year >= 2012'))

    # write PCollection to log file
    #query_results | 'Write to log 1' >> WriteToText('query_results.txt')

    # apply a ParDo to the PCollection 
    names_pcoll = query_results | 'Clean Name' >> beam.ParDo(cleanName())

    # write PCollection to log file
    #names_pcoll | 'Write to log 2' >> WriteToText('CleanedNames.txt')

    #create table
    qualified_table_name = PROJECT_ID + ':Election_Outcome_Clean.President_By_County_Result'
    table_schema = 'year:DATE,state:STRING,state_po:STRING,county:STRING,FIPS:INTEGER,office:STRING,candidate:STRING,party:STRING,candidatevotes:INTEGER,totalvotes:INTEGER,version:INTEGER'

    names_pcoll | 'Write to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(qualified_table_name, 
                                                            schema=table_schema,  
                                                            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))

