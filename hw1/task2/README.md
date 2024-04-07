# VK Education ML 2024
## Task 2 inner join
- shop_product.csv содержит ```product_id \t description```
```
8	Апельсины, кг
2	Капуста свежая, кг
1	Картофель, кг
9	Клубника, кг
3	Лук репчатый, кг
6	Морковь, кг
5	Свекла, кг
7	Яблоки, кг
```
- shop_price.csv содержит ```product_id ; price```
```
8;84.82
2;26.03
1;15.43
7;111.79
3;31.87
6;46.43
5;35.32
7;127.08
10;114.78
1;29.96
```
1. Делаю map - only над каждым файлом, добавляя каждой строчке ему тип информации: price | name
2. Делаю map - reduce задачу.

### Commands inner join
#### Price
```bash
mapred streaming \
-D mapred.reduce.tasks=0 \
-input /home/$USER/data/shop_price.csv \
-output /home/$USER/output/mr3_price \
-mapper mr3_price_mapper.py \
-file /home/$USER/hadoop/script/mr3_price_mapper.py
```

#### Product
```bash
mapred streaming \
-D mapred.reduce.tasks=0 \
-input /home/$USER/data/shop_product.csv \
-output /home/$USER/output/mr3_product \
-mapper mr3_product_mapper.py \
-file /home/$USER/hadoop/script/mr3_product_mapper.py
```

#### Join
```bash
hdfs dfs -mkdir /home/$USER/hadoop/data/join
hdfs dfs -mv /home/$USER/output/mr3_product/part-00000 /home/$USER/hadoop/data/join
```
```bash
mapred streaming \
-D mapred.reduce.tasks=1 \
-input /home/$USER/output/* \
-output /home/$USER/output/m3_join \
-mapper mr3_join_mapper.py \
-reducer mr3_join_reducer.py \
-file /home/$USER/hadoop/script/mr3_join_mapper.py \
-file /home/$USER/hadoop/script/mr3_join_reducer.py
```

#### Check result
```bash
hdfs dfs -cat /home/$USER/output/m3_join/part-00000
```

### Commands local inner join
#### Price
```bash
cat ./hadoop/data/shop_price.csv \
| python3 ./hadoop/script/mr3_price_mapper.py \
> ./hadoop/data/out_price.txt
```
#### Product
```bash
cat ./hadoop/data/shop_product.csv \
| python3 ./hadoop/script/mr3_product_mapper.py \
> ./hadoop/data/out_product.txt
```
#### Join
```bash
cat ./hadoop/data/out.txt \
| python3 ./hadoop/script/mr3_join_mapper.py \
| sort -k1,1 > ./hadoop/data/out_join_mapper.txt
```
```bash
cat ./hadoop/data/out.txt \
| python3 ./hadoop/script/mr3_join_mapper.py \
| sort -k1,1 \
| python3 ./hadoop/script/mr3_join_reducer.py \
> ./hadoop/data/out_join.txt
```
