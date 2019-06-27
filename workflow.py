import datetime
from airflow import models
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

default_dag_args = {
    # https://airflow.apache.org/faq.html#what-s-the-deal-with-start-date
    'start_date': datetime.datetime(2019, 4, 1)
}

###### SQL variables ###### 
raw_dataset = 'Election_Outcome'
new_dataset = 'Election_Outcome_workflow'
sql_cmd_start = 'bq query --use_legacy_sql=false '

sql_HOR = "create table " + new_dataset + ".HOR_Temp as SELECT year, state, state_po, state_fips, state_cen, state_ic, office, stage, special, candidate, party, writein, candidatevotes, totalvotes, version FROM " + raw_dataset + ".House_Of_Rep_Election_Result where year >= 2012"

sql_Senate = "create table " + new_dataset + ".Senate_Temp as SELECT year, state, state_po, state_fips, state_cen, state_ic, office, stage, special, candidate, party, writein, candidatevotes, totalvotes, version FROM " + raw_dataset + ".Senate_Election_Result where year >= 2012"
  
###### Beam variables ######          
LOCAL_MODE=1 # run beam jobs locally
DIST_MODE=2 # run beam jobs on Dataflow

mode=DIST_MODE

if mode == LOCAL_MODE:
    #may need to make a change in that py file
    HOR_and_Senate_script = 'HOR_and_Senate_beam_single.py'
    
if mode == DIST_MODE:
    #same here
    HOR_and_Senate_script = 'HOR_and_Senate_beam_cluster.py'

###### DAG section ###### 
with models.DAG(
        'HOR_and_Senate_workflow',
        schedule_interval=datetime.timedelta(days=1),
        default_args=default_dag_args) as dag:

    ###### SQL tasks ######
    delete_dataset = BashOperator(
            task_id='delete_dataset',
            bash_command='bq rm -r -f Election_Outcome_workflow')
                
    create_dataset = BashOperator(
            task_id='create_dataset',
            bash_command='bq mk Election_Outcome_workflow')
                    
    create_HOR_table = BashOperator(
            task_id='create_HOR_table',
            bash_command=sql_cmd_start + '"' + sql_HOR + '"')

    create_Senate_table = BashOperator(
            task_id='create_Senate_table',
            bash_command=sql_cmd_start + '"' + sql_Senate + '"')
    
    ###### Beam tasks ######     
    HOR_and_Senate_beam = BashOperator(
            task_id='HOR_and_Senate_beam',
            bash_command='python /home/rwbrinson3/.local/bin/dags/HOR_and_Senate_beam_cluster.py')
            
    transition = DummyOperator(task_id='transition')
            
    delete_dataset >> create_dataset >> [create_HOR_table, create_Senate_table] >> transition >> HOR_and_Senate_beam
