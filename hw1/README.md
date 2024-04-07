# VK Education ML 2024

## Hadoop fast setup

### Connect to host
```bash
ssh <login>@89.208.231.195
```
### Upload and copy files to hadoop
```bash
scp <Path_to_file>/hadoop.zip <login>@89.208.231.195:<Path_to_result_dir>
hdfs dfs -copyFromLocal ./hadoop/data /home/$USER/data
```

### Docker
Настроить локально через docker можно по [гайду](https://hub.docker.com/r/cloudera/quickstart/)

```bash
docker run --hostname=quickstart.cloudera --privileged=true -t -i -p 8888 cloudera/quickstart /usr/bin/docker-quickstart
```

### Docker compose [NOT WORKING]
```bash
docker network create shared-vk-network
docker compose up -d
```

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


## Task 1:
Посчитать число встречаемых троек (длину задать в виде константы) букв в словах (порядок не менять). \
Формат вывода: `f"{array_of_letters}-{count}"`. \
Слова короче трех букв - игнорировать. \
Символы, цифры - считать как разделители. \
Оставить только строки, чье число вхождений больше 300  (Задать в качестве константы в коде меппера и/или редьюсера).

### Commands
#### Join
```bash
mapred streaming \
-D mapred.reduce.tasks=1 \
-input /home/$USER/data/books/* \
-output /home/$USER/output/mr4_agg \
-mapper mr4_agg_mapper.py \
-reducer mr4_agg_reducer.py \
-file /home/$USER/hadoop/script/mr4_agg_mapper.py \
-file /home/$USER/hadoop/script/mr4_agg_reducer.py
```

#### Check result
```bash
hdfs dfs -cat /home/$USER/output/mr4_agg/part-00000
```

### Commands local
#### Map
```bash
cat ./hadoop/data/task1.txt \
| python3 ./task1/mr4_agg_mapper.py \
> ./hadoop/data/out_mapper_task1.txt
```

#### Map - reduce
```bash
cat ./hadoop/data/task1.txt \
| python3 ./task1/mr4_agg_mapper.py \
| sort -k1,1 \
| python3 ./task1/mr4_agg_reducer.py \
> ./hadoop/data/out_task1.txt
```