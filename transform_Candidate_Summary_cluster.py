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
            
    new_record = {'cand_id':record.get('cand_id'), 'cand_name':output, 'cand_ici':record.get('cand_ici'), 'pty_cd':record.get('pty_cd'), 'cand_pty_affiliation':record.get('cand_pty_affiliation'), 'ttl_receipts':record.get('ttl_receipts'), 'trans_from_auth':record.get('trans_from_auth'), 'ttl_disb':record.get('ttl_disb'), 'trans_to_auth':record.get('trans_to_auth'), 'coh_bop':record.get('coh_bop'), 'coh_cop':record.get('coh_cop'), 'cand_contrib':record.get('cand_contrib'), 'cand_loans':record.get('cand_loans'), 'other_loans':record.get('other_loans'), 'cand_loan_repay':record.get('cand_loan_repay'), 'other_loan_repay':record.get('other_loan_repay'), 'debts_owed_by':record.get('debts_owed_by'), 'ttl_indiv_contrib':record.get('ttl_indiv_contrib'), 'cand_office_st':record.get('cand_office_st'), 'cand_office_district':record.get('cand_office_district'), 'other_pol_cmte_contrib':record.get('other_pol_cmte_contrib'), 'pol_pty_contrib':record.get('pol_pty_contrib'), 'cvg_end_dt':record.get('cvg_end_dt'), 'indiv_refunds':record.get('indiv_refunds'), 'cmte_refunds':record.get('cmte_refunds'), 'spec_election':record.get('spec_election'), 'prim_election':record.get('prim_election'), 'run_election':record.get('run_election'), 'gen_election':record.get('gen_election'), 'gen_election_precent':record.get('gen_election_precent')}
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
with beam.Pipeline('DataFlowRunner', options=opts) as p:

    query_results = p | 'Read from BigQuery' >> beam.io.Read(beam.io.BigQuerySource(query='SELECT * FROM [not-sure-230422.FEC_Information.Candidate_Summary]'))

    # write PCollection to log file
    #query_results | 'Write to log 1' >> WriteToText('query_results.txt')

    # apply a ParDo to the PCollection 
    names_pcoll = query_results | 'Clean Name' >> beam.ParDo(cleanName())

    # write PCollection to log file
    #names_pcoll | 'Write to log 2' >> WriteToText('CleanedNames.txt')

    #create table
    qualified_table_name = PROJECT_ID + ':FEC_Information.Candidate_Summary_Cleaned'
    table_schema = 'cand_id:STRING,cand_name:STRING,cand_ici:STRING,pty_cd:INTEGER,cand_pty_affiliation:STRING,ttl_receipts:FLOAT,trans_from_auth:FLOAT,ttl_disb:FLOAT,trans_to_auth:FLOAT,coh_bop:FLOAT,coh_cop:FLOAT,cand_contrib:FLOAT,cand_loans:FLOAT,other_loans:FLOAT,cand_loan_repay:FLOAT,other_loan_repay:FLOAT,debts_owed_by:FLOAT,ttl_indiv_contrib:FLOAT,cand_office_st:STRING,cand_office_district:INTEGER,other_pol_cmte_contrib:FLOAT,pol_pty_contrib:FLOAT,cvg_end_dt:TIMESTAMP,indiv_refunds:FLOAT,cmte_refunds:FLOAT,spec_election:STRING,prim_election:STRING,run_election:STRING,gen_election:STRING,gen_election_precent:INTEGER'

    names_pcoll | 'Write to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(qualified_table_name, 
                                                            schema=table_schema,  
                                                            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
