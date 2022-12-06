rm -rf ./results
mkdir results
hdfs dfs -rm -r -f /user/nd1663/project/datasets/median_income_clean
hdfs dfs -rm -r -f /user/nd1663/project/datasets/median_income_clean_short
