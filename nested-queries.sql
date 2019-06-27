--grabs the number of above average donations to each PAC
select ps.cmte_nm, count(transaction_amt) as num_donations
from FEC_Information.FEC_Federal_Campaign_Contributions cc
left outer join FEC_Information.PAC_Summary ps on cc.cmte_id = ps.cmte_id
where Date(ps.cvg_end_dt) > "2018-01-01" and transaction_amt > (select avg(transaction_amt)from FEC_Information.FEC_Federal_Campaign_Contributions)
group by ps.cmte_nm, cc.cmte_id
order by num_donations desc

--grabs the sum of all below average donations for each PAC
select ps.cmte_nm, sum(transaction_amt) as total_donated
from FEC_Information.FEC_Federal_Campaign_Contributions cc
left outer join FEC_Information.PAC_Summary ps on cc.cmte_id = ps.cmte_id
where Date(ps.cvg_end_dt) > "2018-01-01" and transaction_amt < (select avg(transaction_amt)from FEC_Information.FEC_Federal_Campaign_Contributions)
group by ps.cmte_nm, cc.cmte_id
order by total_donated desc

--grabs all the information of the top 10 above average donations
select name, city, state, transaction_amt, occupation
from FEC_Information.FEC_Federal_Campaign_Contributions
where transaction_amt > (select avg(transaction_amt) from FEC_Information.FEC_Federal_Campaign_Contributions)
order by transaction_amt desc
limit 10

--grabs the candidates that have below avg ttl_receipts
select cand_name, ttl_receipts
from FEC_Information.Candidate_Summary
where ttl_receipts < (select avg(ttl_receipts) from FEC_Information.Candidate_Summary)
order by ttl_receipts desc

--This query groups the total amount of candidates (who don't identify as Democrat or Republican) into their respective party
select  cand_pty_affiliation, count(*) as pol_party_count
from `FEC_Information.Candidate_Summary` 
group by cand_pty_affiliation 
having cand_pty_affiliation not in  
(select cand_pty_affiliation 
from `FEC_Information.Candidate_Summary` 
where cand_pty_affiliation = 'DEM' or cand_pty_affiliation = 'REP')
order by pol_party_count desc


--This query finds all candidates who have a total transaction amount larger than the average total transaction amount (Top 10) 
SELECT cand_name, SUM(transaction_amt) as Total
FROM `FEC_Information.Candidate_Summary` cs
JOIN `FEC_Information.Independent_Expenditure` ie ON ie.cand_id = cs.cand_id 
WHERE date(cvg_end_dt) > '2018-01-01'
GROUP BY cand_name
HAVING Total >
(SELECT avg(Amount_Total)
FROM 
(SELECT ie.cand_id, cs.cand_name ,SUM(transaction_amt) AS Amount_Total
FROM FEC_Information.Independent_Expenditure ie
JOIN FEC_Information.Candidate_Summary cs ON ie.cand_id = cs.cand_id 
WHERE date(cvg_end_dt) > '2018-01-01'
GROUP BY ie.cand_id, cs.cand_name
ORDER BY Amount_Total DESC))
ORDER BY Total DESC
LIMIT 10


--This query outputs PACs that had transactions only with candidates with a DEM party affiliation 
SELECT DISTINCT(ie.cmte_id), ps.cmte_nm
FROM `FEC_Information.PAC_Summary` ps 
RIGHT OUTER JOIN `FEC_Information.Independent_Expenditure` ie ON ps.cmte_id = ie.cmte_id 
WHERE ie.cand_id NOT IN
(SELECT DISTINCT(cs.cand_id)
FROM `FEC_Information.Candidate_Summary`  cs
WHERE cs.cand_pty_affiliation != 'DEM')


--This query takes all committees total amount spending and outputs the committees that spent below the average (top 10)
SELECT cmte_id, SUM(transaction_amt) as amt_total
FROM `FEC_Information.Independent_Expenditure` 
GROUP BY cmte_id
HAVING amt_total <
(SELECT AVG(trans_total)
FROM
(SELECT DISTINCT(cmte_id), SUM(transaction_amt) as trans_total
FROM `FEC_Information.Independent_Expenditure` 
GROUP BY cmte_id))
ORDER BY amt_total desc
LIMIT 10
