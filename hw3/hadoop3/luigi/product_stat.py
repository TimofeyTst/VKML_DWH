import random
from collections import defaultdict
from heapq import nlargest

import luigi
import luigi.contrib.hdfs
import luigi.contrib.postgres
import luigi.contrib.spark

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, round, regexp_replace, from_unixtime
from pyspark.sql.types import IntegerType, FloatType, DateType, ShortType, LongType
import pyspark.sql.functions as sf

user = "t.starzhevskij"

current_dt = "2023-03-01"

demography_path = "/user/{}/data/data3/ok/coreDemography".format(user)
country_path = "/user/{}/data/data3/ok/geography/countries.csv".format(user)
rs_city_path = "/user/{}/data/data3/ok/geography/rs_city.csv".format(user)

city_path = "/user/{}/data/data3/rosstat/city.csv".format(user)
product_path = "/user/{}/data/data3/rosstat/product.csv".format(user)
prices_path = "/user/{}/data/data3/rosstat/price/*".format(user)
products_for_stat_path = "/user/{}/data/data3/rosstat/products_for_stat.csv".format(user)

# Путь до результата
output_path = "/user/{}/task4/luigi".format(user)


class ReadCSVSparkTask(luigi.Task):
    city_path = luigi.Parameter()
    product_path = luigi.Parameter()
    prices_path = luigi.Parameter()
    products_for_stat_path = luigi.Parameter()
    demography_path = luigi.Parameter()
    rs_city_path = luigi.Parameter()

    def run(self):
        spark = SparkSession.builder\
            .master("yarn")\
            .appName("t_starzhevsky")\
            .config("spark.executor.instances", "1")\
            .config("spark.executor.memory", "1G")\
            .config("spark.executor.cores", "2")\
            .config("spark.dynamicAllocation.enabled", "false")\
            .config("spark.dynamicAllocation.executorIdleTimeout", "300s")\
            .config("spark.dynamicAllocation.maxExecutors", "1000")\
            .config("spark.driver.memory", "1G")\
            .config("spark.driver.maxResultSize", "1G")\
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")\
            .config("spark.kryoserializer.buffer.max", "1024m")\
            .enableHiveSupport()\
            .getOrCreate()

        # Чтение данных из CSV файлов
        cities_df = (
            spark.read
                .option("header", "false")
                .option("sep", ";")
                .csv(self.city_path)
                .select(
                    col("_c0").alias("city"),
                    col("_c1").cast(IntegerType()).alias("city_id")
                )
        )
        products_df = (
            spark.read
                .option("header", "false")
                .option("sep", ";")
                .csv(product_path)
                .select(
                    col("_c0").alias("product"),
                    col("_c1").cast(IntegerType()).alias("product_id")
                )
        )

        prices_df = (
            spark.read
                .option("header", "false")
                .option("sep", ";")
                .csv(prices_path)
                .select(
                col("_c0").cast(IntegerType()).alias("city_id"),
                col("_c1").cast(IntegerType()).alias("product_id"),
                regexp_replace(
                            col("_c2"), 
                            ",",
                            ".").cast(FloatType()).alias("price")
            )
        )

        products_for_stat_df = (
            spark.read
                .option("header", "false")
                .option("sep", ";")
                .csv(products_for_stat_path)
                .select(
                    col("_c0").cast(IntegerType()).alias("product_id")
            )
        )

        # (user_id, create_date, birth_date (число дней с 1970-01-01), gender, id_country, id_city, login_region)
        users_df = (
            spark.read
                .option("header", "false")
                .option("sep", "\t")
                .csv(demography_path)
                .select(
                    col("_c0").cast(IntegerType()).alias("user_id"),
                    col("_c1").cast(LongType()).alias("create_date"),
                    from_unixtime(col("_c2") * (24 * 60 * 60)).alias('birth_date').cast(DateType()),
                    col("_c2").cast(LongType()).alias("birth_date_days"),
                    col("_c3").cast(ShortType()).alias("gender"),
                    col("_c4").cast(LongType()).alias("country_id"),
                    col("_c5").cast(IntegerType()).alias("city_id"),
                    col("_c6").cast(IntegerType()).alias("login_region")
                )
        )

        rs_city_df = (
            spark.read
                .option("header", "false")
                .option("sep", "\t")
                .csv(rs_city_path)
                .select(
                    col("_c0").cast(IntegerType()).alias("ok_city_id"),
                    col("_c1").cast(IntegerType()).alias("rs_city_id"),
            )
        )
        
        # Сохранение результатов
        cities_df.write.mode("overwrite").parquet(f"{output_path}/tmp/cities.parquet")
        products_df.write.mode("overwrite").parquet(f"{output_path}/tmp/products.parquet")
        prices_df.write.mode("overwrite").parquet(f"{output_path}/tmp/prices.parquet")
        products_for_stat_df.write.mode("overwrite").parquet(f"{output_path}/tmp/products_for_stat.parquet")
        users_df.write.mode("overwrite").parquet(f"{output_path}/tmp/users.parquet")
        rs_city_df.write.mode("overwrite").parquet(f"{output_path}/tmp/rs_city.parquet")

    def output(self):
        return luigi.LocalTarget(f"{output_path}/spark_data_loaded")

class StatisticSparkTask(luigi.Task):
    city_path = luigi.Parameter()
    product_path = luigi.Parameter()
    prices_path = luigi.Parameter()
    products_for_stat_path = luigi.Parameter()
    demography_path = luigi.Parameter()
    rs_city_path = luigi.Parameter()

    def requires(self):
        return ReadCSVSparkTask(
            city_path=self.city_path,
            product_path=self.product_path,
            prices_path=self.prices_path,
            products_for_stat_path=self.products_for_stat_path,
            demography_path=self.demography_path,
            rs_city_path=self.rs_city_path,
        )

    def run(self):
        spark = SparkSession.builder\
            .master("yarn")\
            .appName("t_starzhevsky")\
            .config("spark.executor.instances", "1")\
            .config("spark.executor.memory", "1G")\
            .config("spark.executor.cores", "2")\
            .config("spark.dynamicAllocation.enabled", "false")\
            .config("spark.dynamicAllocation.executorIdleTimeout", "300s")\
            .config("spark.dynamicAllocation.maxExecutors", "1000")\
            .config("spark.driver.memory", "1G")\
            .config("spark.driver.maxResultSize", "1G")\
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")\
            .config("spark.kryoserializer.buffer.max", "1024m")\
            .enableHiveSupport()\
            .getOrCreate()

        cities_df = spark.read.parquet(f"{output_path}/tmp/cities.parquet")
        products_df = spark.read.parquet(f"{output_path}/tmp/products.parquet")
        prices_df = spark.read.parquet(f"{output_path}/tmp/prices.parquet")
        products_for_stat_df = spark.read.parquet(f"{output_path}/tmp/products_for_stat.parquet")
        users_df = spark.read.parquet(f"{output_path}/tmp/users.parquet")
        rs_city_df = spark.read.parquet(f"{output_path}/tmp/rs_city.parquet")

        ## Статистика цен на товары по городам
        price_stat_df = (
            products_for_stat_df
            .join(prices_df, "product_id", how='inner')
            .groupBy(prices_df.product_id)
            .agg(
                sf.round(sf.max(prices_df.price), 2).alias("max_price"),
                sf.round(sf.min(prices_df.price), 2).alias("min_price"),
                sf.round(sf.avg(prices_df.price), 2).alias("avg_price")
            )
            .orderBy(sf.col("max_price").desc())
        )

        ## Города с подсчетом количества продуктов, цены на которые выше средних
        citites_for_stat_df = (
            prices_df
            .join(price_stat_df, "product_id")
            .where(prices_df.price > price_stat_df.avg_price)
            .groupBy(prices_df.city_id)
            .agg(
                sf.count(prices_df.city_id).alias("product_cnt")
            )
            .orderBy(sf.col("product_cnt").desc())
            .join(rs_city_df, rs_city_df.rs_city_id == prices_df.city_id)
            .select(
                sf.col("ok_city_id"),
                sf.col("rs_city_id"),
                sf.col("product_cnt")
            )
        )

        ## Пользователи из требуемых городов со статистикой
        current_date = sf.lit(current_dt).cast(types.DateType())
        age = sf.floor(sf.datediff(current_date, users_df.birth_date) / 365.25).alias('age')

        ok_dem_df = (
            users_df
            .join(citites_for_stat_df, citites_for_stat_df.ok_city_id == users_df.city_id)
            .select(
                citites_for_stat_df.ok_city_id,
                citites_for_stat_df.rs_city_id,
                users_df.user_id,
                users_df.gender,
                age,
            )
            
            .groupBy(citites_for_stat_df.ok_city_id, citites_for_stat_df.rs_city_id)
            .agg(
                sf.count(users_df.user_id).alias("user_cnt"),
                sf.floor(sf.avg(sf.col("age"))).alias("age_avg"),
                sf.sum(sf.when(users_df.gender == 2, 1).otherwise(0)).alias("men_cnt"),
                sf.sum(sf.when(users_df.gender == 1, 1).otherwise(0)).alias("women_cnt")
            )
            .withColumn("men_share", sf.round(sf.col("men_cnt") / sf.col('user_cnt'), 2))
            .withColumn("women_share", sf.round(sf.col("women_cnt") / sf.col('user_cnt'), 2))
        )

        ## Города, с фильтрацией условий для пользователей
        ok_dem_cities_stat_df = (
            ok_dem_df
            .select(
                sf.max(ok_dem_df.age_avg).alias("max_age_avg"),
                sf.min(ok_dem_df.age_avg).alias("min_age_avg"),
                sf.max(ok_dem_df.men_share).alias("max_men_share"),
                sf.max(ok_dem_df.women_share).alias("max_women_share")
            )
            .first()
        )

        # Фильтрация городов с максимальным и минимальным средним возрастом
        max_age_avg_cities = ok_dem_df.where(ok_dem_df.age_avg == ok_dem_cities_stat_df.max_age_avg)
        min_age_avg_cities = ok_dem_df.where(ok_dem_df.age_avg == ok_dem_cities_stat_df.min_age_avg)

        # Фильтрация городов с максимальной долей мужчин и максимальной долей женщин
        max_men_share_cities = ok_dem_df.where(ok_dem_df.men_share == ok_dem_cities_stat_df.max_men_share)
        max_women_share_cities = ok_dem_df.where(ok_dem_df.women_share == ok_dem_cities_stat_df.max_women_share)

        # Объединение всех фильтраций в один датафрейм
        all_filtered_cities = (
            max_age_avg_cities
            .unionAll(min_age_avg_cities)
            .unionAll(max_men_share_cities)
            .unionAll(max_women_share_cities)
            .distinct()
        )

        ## Статистика цен на товары по требуемым городам
        city_product_stat_df = (
            all_filtered_cities
            .join(prices_df, prices_df.city_id == all_filtered_cities.rs_city_id, how='inner')
            .join(products_for_stat_df, "product_id", how="inner")

            .groupBy(all_filtered_cities.ok_city_id, all_filtered_cities.rs_city_id)
            .agg(
                sf.min(sf.col("price")).alias("cheapest_price"),
                sf.max(sf.col("price")).alias("most_expensive_price")
            )
        )

        cheapest_product_df = (
            all_filtered_cities
            .join(prices_df, prices_df.city_id == all_filtered_cities.rs_city_id, how='inner')
            .join(products_for_stat_df, "product_id", how="inner")

            .join(products_df, "product_id", how="inner")
            .join(city_product_stat_df, "rs_city_id")
            
            .where(sf.col("price") == sf.col("cheapest_price"))
            .withColumnRenamed("product", "cheapest_product_name")
            .select(
                sf.col("rs_city_id"),
                sf.col("cheapest_price"),
                sf.col("cheapest_product_name")
            )
        )

        most_expensive_product_df = (
            all_filtered_cities
            .join(prices_df, prices_df.city_id == all_filtered_cities.rs_city_id, how='inner')
            .join(products_for_stat_df, "product_id", how="inner")
            
            .join(products_df, "product_id", how="inner")
            .join(city_product_stat_df, "rs_city_id")
            
            .where(sf.col("price") == sf.col("most_expensive_price"))
            .withColumnRenamed("product", "most_expensive_product_name")
            .select(
                sf.col("rs_city_id"),
                sf.col("most_expensive_price"),
                sf.col("most_expensive_product_name")
            )
        )

        result_df = (
            cheapest_product_df
            .join(most_expensive_product_df, "rs_city_id", how='inner')
            .join(cities_df, sf.col("city_id") == sf.col("rs_city_id"))
            .withColumn(
                "price_difference",
                sf.round(most_expensive_product_df.most_expensive_price - cheapest_product_df.cheapest_price, 2)
            )
            .select(
                sf.col("city").alias("city_name"),
                sf.col("cheapest_product_name"),
                sf.col("most_expensive_product_name"),
                sf.col("price_difference")
            )
        )

        price_stat_df.write.mode("overwrite").parquet(f"{output_path}/tmp/products_for_stat.parquet")
        citites_for_stat_df.write.mode("overwrite").parquet(f"{output_path}/citites_for_stat_df.parquet")
        ok_dem_df.write.mode("overwrite").parquet(f"{output_path}/ok_dem.parquet")
        all_filtered_cities.write.mode("overwrite").parquet(f"{output_path}/tmp/all_filtered_cities.parquet")
        result_df.write.mode("overwrite").parquet(f"{output_path}/price_stat.parquet")

        with self.output().open("w") as output_file:
            # Write final result to output file
            for city, cheapest, expensive, price in result_df:
                out_line = ';'.join([
                    city,
                    cheapest,
                    expensive,
                    str(price)
                ])
                output_file.write((out_line + '\n'))


    def output(self):
        return luigi.LocalTarget(f"{output_path}/price_stat.csv")

if __name__ == "__main__":
    luigi.run()
