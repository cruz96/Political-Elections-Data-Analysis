---------------------------------------------------------------------

--Creating Independent_Expenditure zip_code had wrong type and file_num and name were not in same order
SELECT * 
  EXCEPT(zip_code, file_num, name)
from 
  `Independent_Expenditure.2012`
Union distinct
SELECT * 
  EXCEPT(zip_code, file_num, name)
from 
  `Independent_Expenditure.2014`
Union distinct
SELECT * 
  EXCEPT(zip_code, file_num, name)
from 
  `Independent_Expenditure.2016`
  Union distinct
SELECT * 
  EXCEPT(zip_code, transaction_dt_raw, file_num, name)
from 
  `Independent_Expenditure.2018`
  
--Create another table with zip_code, name, file_num, and primary key sub_id to join on Independent_Expenditure
Select CAST(zip_code AS STRING) AS zip_code, name, file_num, sub_id
from `Independent_Expenditure.2012`
Union all
Select CAST(zip_code AS STRING) AS zip_code, name, file_num, sub_id
from `Independent_Expenditure.2014`
Union all
Select CAST(zip_code AS STRING) AS zip_code, name, file_num, sub_id
from `Independent_Expenditure.2016`
Union all 
Select CAST(zip_code AS STRING) AS zip_code, name, file_num, sub_id
from `Independent_Expenditure.2018`
  
--Query to join the two tables together
select * except(zip_code,sub_id), g.sub_id, g.zip_code
from `FEC_Information.Independent_Expenditure_copy` c
inner join `FEC_Information.independenttest` g on c.sub_id = g.sub_id

--------------------------------------------------------------

--Combined all years for PAC_Summary
select *
from `PAC_Summary.2012` 
Union distinct
select * 
from `PAC_Summary.2014` 
Union distinct 
select *
from `PAC_Summary.2016` 
Union distinct 
select * 
from `PAC_Summary.2018`

------------------------------------------------------------

--Combined all years for Candidate_Summary, converting all gen_election_precent columns into an INT64 type
SELECT * EXCEPT (gen_election_precent), 
CAST(gen_election_precent AS INT64) AS gen_election_precent 
FROM `Candidate_Summary.2012`  

UNION DISTINCT

SELECT * EXCEPT (gen_election_precent), 
CAST(gen_election_precent AS INT64) AS gen_election_precent 
FROM `Candidate_Summary.2014`  

UNION DISTINCT

SELECT * EXCEPT (gen_election_precent), 
CAST(gen_election_precent AS INT64) AS gen_election_precent 
FROM `Candidate_Summary.2016` 

UNION DISTINCT

SELECT * EXCEPT (gen_election_precent), 
CAST(gen_election_precent AS INT64) AS gen_election_precent 
FROM `Candidate_Summary.2018`

------------------------------------------------------------------

--Combined all cleaned years of FEC_Federal_Campaign_Contribution
SELECT cmte_id, amndt_ind, rpt_tp, transaction_pgi, transaction_tp, entity_tp, name,
city, state, zip_code, employer, occupation, transaction_amt, other_id, tran_id,
file_num, memo_cd, memo_text, sub_id,image_num, transaction_dt
From `FEC_Federal_Campaign_Contribution.2012_2014_2016_combined_cleaned` 
Union distinct
Select *
From `FEC_Federal_Campaign_Contribution.2018_cleaned`

-------------------------------------------------------------------

--Combines all records of the columns that the 4 years of Congressional_Candidate_Disbursements share in common
SELECT cmte_id, amndt_ind, rpt_tp, image_num, name, city, state, zip_code, transaction_dt, transaction_amt, transaction_pgi,
memo_cd, memo_text, entity_tp, sub_id, file_num, tran_id
FROM `Congressional_Candidate_Disbursements.2012` 

UNION ALL

SELECT cmte_id, amndt_ind, rpt_tp, image_num, name, city, state, zip_code, transaction_dt, transaction_amt, transaction_pgi,
memo_cd, memo_text, entity_tp, sub_id, file_num, tran_id
FROM `Congressional_Candidate_Disbursements.2014` 

UNION ALL

SELECT cmte_id, amndt_ind, rpt_tp, image_num, name, city, state, zip_code, transaction_dt, transaction_amt, transaction_pgi,
memo_cd, memo_text, entity_tp, sub_id, file_num, tran_id
FROM `Congressional_Candidate_Disbursements.2016` 

UNION ALL

SELECT cmte_id, amndt_ind, rpt_tp, image_num, name, city, state, zip_code, transaction_dt, transaction_amt, transaction_pgi,
memo_cd, memo_text, entity_tp, sub_id, file_num, tran_id
FROM `Congressional_Candidate_Disbursements.2018` 
