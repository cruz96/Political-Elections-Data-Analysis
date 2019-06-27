--This grabs the total votes received and the total transactions from each president
SELECT ie.cand_id, cs.cand_name, 
(Select SUM(pbs.candidatevotes) from `not-sure-230422`.Election_Outcome_Clean.President_By_State_Result pbs where candidate = cs.cand_name group by pbs.candidate) AS Total_Votes, 
SUM(transaction_amt) AS Amount_Total
FROM (`not-sure-230422`.FEC_Information.Independent_Expenditure ie
JOIN `not-sure-230422`.FEC_Information.Candidate_Summary_Cleaned cs ON ie.cand_id = cs.cand_id )
WHERE date(cvg_end_dt) > '2018-01-01'
GROUP BY ie.cand_id, cs.cand_name
ORDER BY Amount_Total desc;
limit 10

--This grabs the PACs who donated to the most voted candidate
select distinct ps.cmte_nm from `not-sure-230422.FEC_Information.Independent_Expenditure` ie inner join `not-sure-230422.FEC_Information.PAC_Summary` ps on ie.cmte_id = ps.cmte_id
where cand_id = (select cand_id from (Select SUM(pbs.candidatevotes) as TotalVotes, candidate from `not-sure-230422`.Election_Outcome_Clean.President_By_State_Result pbs group by pbs.candidate order by TotalVotes desc limit 1) 
inner join `not-sure-230422.FEC_Information.Candidate_Summary_Cleaned` cc on candidate = cc.cand_name
WHERE date(cvg_end_dt) > '2018-01-01')

--This grabs the total donated for each PAC that donated to the most voted candidate
select sum(ie.transaction_amt) as Total_Donated, ps.cmte_nm from `not-sure-230422.FEC_Information.Independent_Expenditure` ie inner join `not-sure-230422.FEC_Information.PAC_Summary` ps on ie.cmte_id = ps.cmte_id
where cand_id = (select cand_id from (Select SUM(pbs.candidatevotes) as TotalVotes, candidate from `not-sure-230422`.Election_Outcome_Clean.President_By_State_Result pbs group by pbs.candidate order by TotalVotes desc limit 1) 
inner join `not-sure-230422.FEC_Information.Candidate_Summary_Cleaned` cc on candidate = cc.cand_name
WHERE date(cvg_end_dt) > '2018-01-01') and date(cvg_end_dt) > '2018-01-01'
group by ps.cmte_nm
order by Total_Donated desc

--This query outputs all candidates from the House of Representatives and the Senate who did not receive any PAC money whatsoever. 
SELECT DISTINCT candidate
FROM `not-sure-230422.Election_Outcome_Clean.HOR_and_Senate_Result`  
WHERE candidate NOT IN 
(SELECT cand_name 
FROM `not-sure-230422.FEC_Information.Candidate_Summary_Cleaned`)

--This query counts total votes received from Senate candidates who did not receive any PAC money. 
SELECT DISTINCT candidate, office, SUM(candidatevotes) AS TotalVotes
FROM `not-sure-230422.Election_Outcome_Clean.HOR_and_Senate_Result`  
WHERE office = "US Senate" AND candidate NOT IN 
(SELECT cand_name 
FROM `not-sure-230422.FEC_Information.Candidate_Summary_Cleaned`)
GROUP BY candidate, office
ORDER BY TotalVotes desc
LIMIT 10

--This query counts total votes received from House of Representative candidates who did not receive any PAC money. 
SELECT DISTINCT candidate, office, SUM(candidatevotes) AS TotalVotes
FROM `not-sure-230422.Election_Outcome_Clean.HOR_and_Senate_Result`  
WHERE office = "US House" AND candidate NOT IN 
(SELECT cand_name 
FROM `not-sure-230422.FEC_Information.Candidate_Summary_Cleaned`)
GROUP BY candidate, office
ORDER BY TotalVotes desc
LIMIT 10
