--grabs the number of donations for each PAC
select ps.cmte_nm, count(transaction_amt) as num_donations
from FEC_Information.FEC_Federal_Campaign_Contributions cc
left outer join FEC_Information.PAC_Summary ps on cc.cmte_id = ps.cmte_id
where Date(ps.cvg_end_dt) > "2018-01-01"
group by ps.cmte_nm, cc.cmte_id
order by num_donations desc

--grabs the sum of all donations for each PAC
select ps.cmte_nm, sum(transaction_amt) as total_donated
from FEC_Information.FEC_Federal_Campaign_Contributions cc
left outer join FEC_Information.PAC_Summary ps on cc.cmte_id = ps.cmte_id
where Date(ps.cvg_end_dt) > "2018-01-01"
group by ps.cmte_nm, cc.cmte_id
order by total_donated desc

--grabs the total donated for any PAC's with names that have DEM or REP
select ps.cmte_nm, sum(transaction_amt) as total_donated
from FEC_Information.FEC_Federal_Campaign_Contributions cc
left outer join FEC_Information.PAC_Summary ps on cc.cmte_id = ps.cmte_id
where Date(ps.cvg_end_dt) > "2018-01-01"
group by ps.cmte_nm, cc.cmte_id
having ps.cmte_nm like "%DEM%" or ps.cmte_nm like "%REP%"
order by total_donated desc

--grabs the num of expenditures from independents with PAC's that have names with DEM or REP
select ps.cmte_nm, count(transaction_amt) as num_expenditures
from FEC_Information.Independent_Expenditure cc
left outer join FEC_Information.PAC_Summary ps on cc.cmte_id = ps.cmte_id
where Date(ps.cvg_end_dt) > "2018-01-01"
group by ps.cmte_nm, cc.cmte_id
having ps.cmte_nm like "%DEM%" or ps.cmte_nm like "%REP%"
order by num_expenditures desc

--This query groups total transaction amounts with their corresponding candidate. The top 10 receivers are outputted  
SELECT ie.cand_id, cs.cand_name ,SUM(transaction_amt) AS Amount_Total
FROM FEC_Information.Independent_Expenditure ie
JOIN FEC_Information.Candidate_Summary cs ON ie.cand_id = cs.cand_id 
WHERE date(cvg_end_dt) > '2018-01-01'
GROUP BY ie.cand_id, cs.cand_name
ORDER BY Amount_Total desc
LIMIT 10

--This query counts the total number of individual transactions for the top 10 individual transaction receivers
SELECT ie.cand_id, cs.cand_name ,COUNT(transaction_amt) AS Transaction_Count
FROM FEC_Information.Independent_Expenditure ie
JOIN FEC_Information.Candidate_Summary cs ON ie.cand_id = cs.cand_id 
WHERE date(cvg_end_dt) > '2018-01-01'
GROUP BY ie.cand_id, cs.cand_name
ORDER BY Transaction_Count desc
LIMIT 10

--This query counts how many contributions over $500 individuals have made and groups the totals by state 
SELECT state, COUNT (transaction_amt) as Transaction_Count
FROM FEC_Information.FEC_Federal_Campaign_Contributions
WHERE transaction_amt >= 500
GROUP BY state

--This query counts how many times people have donated, grouped into job types of people living in Austin
SELECT occupation, COUNT(*) as Job_Type_Total
FROM FEC_Information.FEC_Federal_Campaign_Contributions 
WHERE city = 'AUSTIN'
GROUP BY occupation
HAVING occupation NOT LIKE 'RETIRED' AND occupation NOT LIKE 'NOT EMPLOYED'
AND occupation NOT LIKE 'UNEMPLOYED' AND occupation NOT LIKE 'NONE' AND occupation NOT LIKE 'NOT-EMPLOYED'
ORDER BY Job_Type_Total DESC
