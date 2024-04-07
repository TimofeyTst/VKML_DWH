# VK Education ML 2024

## Task
Папка сoreDemography содержат данные демографии пользователей ОК. В info.txt - описание полей.
- Написать запрос рассчитывающий распределение дней рождения по месяцам. В результате должна получится таблица из двух колонок (month, cnt).

Решение должно содержать файлы с кодом на создание внешней таблицы (в
своей базе данных) и сам запрос.

## Create user table
```bash
create database user_t_starzhevskij
location '/home/t.starzhevskij/warehouse/';
```

```bash
use user_t_starzhevskij;
```

## Upload files to hdfs
```bash
hdfs dfs -copyFromLocal ./data2 /home/$USER/warehouse
```

## Create external table
```bash
create external table users (
    userId bigint,
    create_date bigint,
    birth_date int,
    gender int,
    ID_country bigint,
    ID_Location bigint,
    loginRegion bigint
)
row format delimited
fields terminated by '\t' 
LOCATION '/home/t.starzhevskij/warehouse/data2/coreDemography';
```

## Join task
### Version 1
```bash
SELECT
    MONTH(from_unixtime(birth_date * 86400)) AS month,
    COUNT(*) AS cnt
FROM 
    users
GROUP BY 
    MONTH(from_unixtime(birth_date * 86400))
ORDER BY
    month;
```
### Version 2
```bash
SELECT
    month,
    COUNT(*) as cnt
FROM (
    SELECT 
        MONTH(from_unixtime(birth_date * 86400)) AS month
    FROM 
        users
) subquery
GROUP BY month
ORDER BY month;
```
### Version 3
> Не знаю почему, но этот вариант дает другой результат, который совпадает с ответом в лекции
```bash
SELECT 
    MONTH(date_add('1970-01-01', birth_date)) AS month,
    COUNT(*) AS cnt
FROM 
    users
GROUP BY 
    MONTH(date_add('1970-01-01', birth_date))
ORDER BY
    month;
```



## Result
### Version 1-2
```
NULL    9
1       10289
2       8508
3       9236
4       8865
5       9053
6       9347
7       9537
8       9217
9       8659
10      8541
11      8056
12      8248
```
### Version 3
```
NULL    9
1       10297
2       8496
3       9242
4       8875
5       9051
6       9332
7       9534
8       9228
9       8653
10      8534
11      8061
12      8253
```
