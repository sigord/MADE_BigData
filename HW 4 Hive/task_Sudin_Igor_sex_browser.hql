set hive.auto.convert.join=false;
set mapreduce.job.reduces=3;

-- USE bdmade2022q2_sudin;
SELECT users.browser, sum(if(users.sex="male",1,0)), sum(if(users.sex="female",1,0)) 
FROM logs JOIN users ON logs.ip = users.ip GROUP BY users.browser
LIMIT 10;