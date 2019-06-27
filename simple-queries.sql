--Candidate_Summary
--Grabs the Candidate names, party, state they are running for, and orders by the most personal contributions from all years
select cand_name, cand_pty_affiliation, cand_office_st, ttl_indiv_contrib 
from `FEC_Information.Candidate_Summary` 
where cand_pty_affiliation = "DEM" or cand_pty_affiliation = "REP" 
order by ttl_indiv_contrib desc

--Grabs the Candidate names, party, state they are running for, and orders by the most political party contributions from all years
select cand_name, cand_pty_affiliation, cand_office_st, pol_pty_contrib 
from `FEC_Information.Candidate_Summary` 
where cand_pty_affiliation = "DEM" or cand_pty_affiliation = "REP" 
order by pol_pty_contrib desc

--Grabs the Candidate names, party, debts owed, for everyone running for president and orders by the most individual contributions
select cand_name, cand_pty_affiliation, cand_office_st, debts_owed_by, ttl_indiv_contrib
from `FEC_Information.Candidate_Summary` 
where  cand_office_st = "00"
order by ttl_indiv_contrib desc

--Grabs the Candidate names, party, for everyone running in Texas, and orders by the most debts owed from all years
select cand_name, cand_pty_affiliation, cand_office_st, debts_owed_by, ttl_indiv_contrib
from `FEC_Information.Candidate_Summary` 
where  cand_office_st = "TX"
order by debts_owed_by desc

------------------------------------------------------------------------------

--Congressional_Candidate_Disbursements
--Grabs the name, city, state, amount, and purpose of the biggest donations in 2012
select name, city, state, transaction_amt, purpose
from `Congressional_Candidate_Disbursements.2012` 
where category_desc = "Donations"
order by transaction_amt desc

--Grabs the name, city, amount, and purpose of the biggest disbursements in Texas during 2014
select name, city, transaction_amt, purpose
from `Congressional_Candidate_Disbursements.2014` 
where state = "TX"
order by transaction_amt desc

--Grabs the name, state, amount, and memo of the biggest charges in 2016
select name, state, transaction_amt, memo_text
from `Congressional_Candidate_Disbursements.2016` 
where memo_text != "null" and transaction_amt < 0
order by transaction_amt asc

--Grabs the name, state, amount, and memo of the smallest charges in 2018
select name, state, transaction_amt, memo_text
from `Congressional_Candidate_Disbursements.2018` 
where memo_text != "null" and transaction_amt < 0
order by transaction_amt desc

------------------------------------------------------------------------------

--FEC_Federal_Campaign_Contribution
--Grabs the name, state, employer, transaction amount of the biggest transactions with memos from 2012
select name, state, employer, transaction_amt, memo_text
from `FEC_Federal_Campaign_Contribution.2012`
where memo_text != "null"
order by transaction_amt desc

--This guy had a lot of expensive transactions so it looks at all of his from 2012
select name, state, employer, transaction_amt, memo_text
from `FEC_Federal_Campaign_Contribution.2012`
where memo_text != "null" and name = "DEWHURST, DAVID"
order by transaction_amt desc

--Grabs the name, state, employer, transaction_amt, and memo text of all transactions with UT as the employer
select name, state, employer, transaction_amt, memo_text
from `FEC_Federal_Campaign_Contribution.2014`
where memo_text != "null" and employer = "UNIVERSITY OF TEXAS"
order by transaction_amt desc

--Grabs the name, state, employer, occupation, transaction_amt, memo_text of any transactions made by people with Brinson in their name from 2014
select name, state, employer, occupation, transaction_amt, memo_text
from `FEC_Federal_Campaign_Contribution.2014`
where name LIKE '%BRINSON%'
order by name

--This query finds names of contributors who contributed in Texas, Ohio, and Alabama
SELECT sub_id, cmte_id, name, city, state, zip_code, employer
FROM `FEC_Federal_Campaign_Contribution.2016`
WHERE state in ('TX', 'OH', 'AL')
ORDER BY state

--This query finds all contributors who contributed over $2000 from greatest amount to least amount
SELECT sub_id, cmte_id, name, city, state, zip_code, employer, transaction_amt
FROM `FEC_Federal_Campaign_Contribution.2018`
WHERE transaction_amt >= 2000
ORDER BY transaction_amt desc

------------------------------------------------------------------------------

--Independent_Expenditure
--This query finds all entity CCM types and orders them by zip code
SELECT cmte_id, entity_tp, city, state
FROM `FEC_Information.Independent_Expenditure`
WHERE entity_tp = 'CCM'
ORDER BY zip_code

--This query finds all entity COM types that contributed over $500 and orders them by most money to least money
SELECT cmte_id, entity_tp, transaction_amt, name
FROM `FEC_Information.Independent_Expenditure`
WHERE entity_tp = 'COM' and transaction_amt >= 500
ORDER BY transaction_amt desc

--This query finds all contributors in alphabetical order who donated from Washington or Anchorage
SELECT cmte_id, city, state, name
FROM `FEC_Information.Independent_Expenditure`
WHERE city = 'Washington' or city = 'Anchorage' 
ORDER BY name

--This query finds all zip codes that start with a 9 and orders by the entity type
SELECT cmte_id, entity_tp, zip_code
FROM `FEC_Information.Independent_Expenditure`
WHERE zip_code LIKE '9%'
ORDER BY entity_tp

------------------------------------------------------------------------------

--PAC_Summary
--This query shows which committee of debt from greatest amount to least (limit of 10)
SELECT cmte_id, cmte_nm, debts_owed_by
FROM `FEC_Information.PAC_Summary`
WHERE debts_owed_by > 0
ORDER BY debts_owed_by desc 
LIMIT 10

--This query shows contributions from individuals that made contributions greater than $400,000 (limit of 20)
SELECT cmte_id, cmte_nm, indv_contrib
FROM `FEC_Information.PAC_Summary`
WHERE indv_contrib > 400000
ORDER BY indv_contrib desc 
LIMIT 20

--This query shows a list of committee types from committees that start with the letter 'A' in ascending order
SELECT cmte_id, cmte_nm, cmte_tp
FROM `FEC_Information.PAC_Summary`
WHERE cmte_nm LIKE 'A%'
ORDER BY cmte_nm

--This query shows a list of other political action committee contributions and orders them in a descending order 
SELECT cmte_id, cmte_nm, other_pol_cmte_contrib
FROM `FEC_Information.PAC_Summary`
WHERE other_pol_cmte_contrib > 3000
ORDER BY other_pol_cmte_contrib desc

