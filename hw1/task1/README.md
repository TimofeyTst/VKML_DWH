# VK Education ML 2024

## Hadoop fast setup

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