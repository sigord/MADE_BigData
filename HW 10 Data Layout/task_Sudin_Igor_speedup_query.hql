--USE bdmade2022q2_sudin;
SELECT COUNT(*) FROM ${table_name} WHERE user_agent = 'Chrome/5.0' and page_size < 1000 and page_size >= 2000;