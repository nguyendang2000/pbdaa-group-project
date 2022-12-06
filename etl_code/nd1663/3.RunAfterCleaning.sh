hdfs dfs -get /user/nd1663/project/datasets/median_income_clean/part-00000 ./results/clean_data.csv
hdfs dfs -get /user/nd1663/project/datasets/median_income_clean_short/part-00000 ./results/clean_data_short.csv

touch ./results/clean_data_with_headers.csv
cat ./headers_long.txt > ./results/clean_data_with_headers.csv
cat ./results/clean_data.csv >> ./results/clean_data_with_headers.csv

touch ./results/clean_data_with_headers_short.csv
cat ./headers_short.txt > ./results/clean_data_with_headers_short.csv
cat ./results/clean_data_short.csv >> ./results/clean_data_with_headers_short.csv

head ./results/clean_data_with_headers_short.csv