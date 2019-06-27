--This query joins 3 tables and shows sub_id is unique in all of them
select cc.sub_id
from (`FEC_Federal_Campaign_Contribution.2012` cs
inner join `Congressional_Candidate_Disbursements.2012` cc
on cs.sub_id = cc.sub_id)
inner join `FEC_Information.Independent_Expenditure` cx
on cc.sub_id = cx.sub_id
order by sub_id

--This query joins a list of transactions with the list of candidates to see who had a transaction with Donald Trump
select distinct name, cs.cand_name, ie.transaction_amt, memo_text
from FEC_Information.Independent_Expenditure ie
right outer join FEC_Information.Candidate_Summary cs
on ie.cand_id = cs.cand_id
where cs.cand_name Like "TRUMP%" and ie.name != "null"
order by transaction_amt desc

--This query sees the name of the commitee contributed to by anyone who worked at UT in 2012
select ps.cmte_id, ps.cmte_nm, cc.name, cc.employer, transaction_amt
from FEC_Information.PAC_Summary ps
inner join FEC_Federal_Campaign_Contribution.2012 cc on ps.cmte_id = cc.cmte_id
where cc.employer = "UNIVERSITY OF TEXAS"
order by transaction_amt

--This query sees all donations and political contributions and grabs the PAC name and the name of the transactioner in 2014
select ps.cmte_id, ps.cmte_nm, cc.name, transaction_amt
from FEC_Information.PAC_Summary ps
inner join Congressional_Candidate_Disbursements.2014 cc on ps.cmte_id = cc.cmte_id
where cc.category_desc = "Donations" or cc.category_desc = "Political Contributions"
order by transaction_amt

--this query displays transactions and their amounts made from PACs and to which specific candidate it went to 
SELECT ps.cmte_nm, ccd.transaction_amt, ccd.cand_id 
FROM `FEC_Information.PAC_Summary` ps
LEFT OUTER JOIN `Congressional_Candidate_Disbursements.2018` ccd
ON ps.cmte_id = ccd.cmte_id 
ORDER BY ccd.transaction_amt DESC

--this query displays all committee names and the type of committee they are (6 different kinds)
SELECT ps.cmte_nm, ccd.entity_tp 
FROM `FEC_Information.PAC_Summary` ps
LEFT OUTER JOIN `Congressional_Candidate_Disbursements.2018` ccd
ON ps.cmte_id = ccd.cmte_id 
ORDER BY ccd.entity_tp DESC
