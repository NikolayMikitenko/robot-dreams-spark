from pyspark.sql import SparkSession
from pyspark.sql.functions import asc, desc, concat, col, lit, unix_timestamp, row_number, rank
from pyspark.sql.functions import sum as fsum
from pyspark.sql.functions import count as fcount
from pyspark.sql.window import Window

spark = SparkSession.builder\
            .config('spark.driver.extraClassPath', '/home/user/shared/postgresql-42.3.1.jar')\
            .master('local')\
            .appName('homework_lesson_13')\
            .getOrCreate()

spark

pg_url = 'jdbc:postgresql://127.0.0.1/postgres'
pg_creds = {'user' : 'pguser', 'password' : 'secret'}

df_category = spark.read.jdbc(pg_url, table='category', properties=pg_creds)
df_film_category = spark.read.jdbc(pg_url, table='film_category', properties=pg_creds)
df_actor = spark.read.jdbc(pg_url, table='actor', properties=pg_creds)
df_film_actor = spark.read.jdbc(pg_url, table='film_actor', properties=pg_creds)
df_inventory = spark.read.jdbc(pg_url, table='inventory', properties=pg_creds)
df_rental = spark.read.jdbc(pg_url, table='rental', properties=pg_creds)
df_payment = spark.read.jdbc(pg_url, table='payment', properties=pg_creds)
df_film = spark.read.jdbc(pg_url, table='film', properties=pg_creds)
df_city = spark.read.jdbc(pg_url, table='city', properties=pg_creds)
df_address = spark.read.jdbc(pg_url, table='address', properties=pg_creds)
df_customer = spark.read.jdbc(pg_url, table='customer', properties=pg_creds)

print('1. вывести количество фильмов в каждой категории, отсортировать по убыванию.')
df1 = df_category.join(df_film_category
                 , df_film_category.category_id == df_category.category_id
                 , 'inner')\
    .groupby(df_category.name)\
    .count()\
    .withColumnRenamed("name", "category")\
    .withColumnRenamed("count", "films_qauntity")
df1.orderBy(desc('films_qauntity')).select(df1.category, df1.films_qauntity).show()
print('\n')
print('\n')

print('2. вывести 10 актеров, чьи фильмы большего всего арендовали, отсортировать по убыванию.')
df_actor.select('actor_id', concat(col('first_name'), lit(' '), col('last_name')).alias('actor'))\
    .join(
    df_film_actor
    , df_film_actor.actor_id == df_actor.actor_id
    , 'inner'
    )\
    .join(
    df_inventory
    , df_inventory.film_id == df_film_actor.film_id
    , 'inner'
    )\
    .join(
    df_rental
    , df_rental.inventory_id == df_inventory.inventory_id
    , 'inner'
    )\
    .select('actor', 'rental_id')\
    .groupby('actor')\
    .count()\
    .orderBy(desc('count'))\
    .select('actor')\
    .show(10)
print('\n')
print('\n')

print('3. вывести категорию фильмов, на которую потратили больше всего денег.')
df_category.join(df_film_category, df_film_category.category_id == df_category.category_id, 'inner')\
    .select(col('name').alias('category'), 'film_id')\
    .join(df_inventory, df_inventory.film_id == df_film_category.film_id, 'inner')\
    .join(df_rental, df_rental.inventory_id == df_inventory.inventory_id, 'inner')\
    .join(df_payment, df_payment.rental_id == df_rental.rental_id, 'inner')\
    .groupBy('category')\
    .sum('amount')\
    .orderBy(desc('sum(amount)'))\
    .select('category')\
    .show(1)
print('\n')
print('\n')

print('4. вывести названия фильмов, которых нет в inventory. Написать запрос без использования оператора IN.')
df_film.join(df_inventory, df_inventory.film_id == df_film.film_id, 'leftanti')\
    .groupby('title')\
    .count()\
    .select('title')\
    .show(1000000)
print('\n')
print('\n')

print('''
5. вывести топ 3 актеров, которые больше всего появлялись в фильмах в категории “Children”. 
Если у нескольких актеров одинаковое кол-во фильмов, вывести всех.
''')
qw = Window.orderBy(desc("count"))
df_actor.join(df_film_actor, df_film_actor.actor_id == df_actor.actor_id, 'inner')\
    .join(df_film_category, df_film_category.film_id == df_film_actor.film_id, 'inner')\
    .join(df_category, (df_category.category_id == df_film_category.category_id) & (df_category.name == 'Children'), 'inner')\
    .withColumn('actor', concat(col('first_name'), lit(' '), col('last_name')))\
    .groupby('actor')\
    .count()\
    .sort(desc('count'))\
    .withColumn("position", rank().over(qw))\
    .where(col('position') <= 3)\
    .select('actor')\
    .show()
print('\n')
print('\n')

print('''
6. вывести города с количеством активных и неактивных клиентов (активный — customer.active = 1). 
Отсортировать по количеству неактивных клиентов по убыванию.
''')
df_city.join(df_address, df_address.city_id == df_city.city_id, 'inner')\
    .join(df_customer, df_customer.address_id ==df_address.address_id, 'inner')\
    .groupBy('city')\
    .agg(fsum('active').alias('active_customer_quantity'), fcount('active').alias('all_customer_quantity'))\
    .select('city', 'active_customer_quantity', (col('all_customer_quantity') - col('active_customer_quantity')).alias('inactive_customer_quantity'))\
    .sort(desc('inactive_customer_quantity'))\
    .show(1000000)
print('\n')
print('\n')

print('''
7. вывести категорию фильмов, у которой самое большое кол-во часов суммарной аренды в городах 
(customer.address_id в этом city), и которые начинаются на букву “a”. 
То же самое сделать для городов в которых есть символ “-”. Написать все в одном запросе.
''')
w = Window.partitionBy("city").orderBy(desc("sum(rent_time)"))
df_city.filter(col("city").like("a%") | col("city").like("A%") | col("city").like("%-%"))\
    .join(df_address, df_address.city_id == df_city.city_id, 'inner')\
    .join(df_customer, df_customer.address_id == df_address.address_id, 'inner')\
    .join(df_rental, df_rental.customer_id == df_customer.customer_id, 'inner')\
    .where(df_rental.rental_date.isNotNull() & df_rental.return_date.isNotNull())\
    .join(df_inventory, df_inventory.inventory_id == df_rental.inventory_id, 'inner')\
    .join(df_film_category, df_film_category.film_id == df_inventory.film_id, 'inner')\
    .join(df_category, df_category.category_id == df_film_category.category_id , 'inner')\
    .withColumn("rent_time", (unix_timestamp('return_date') - unix_timestamp("rental_date")))\
    .select('city', df_category.name.alias('category'), 'rent_time')\
    .groupby('city', 'category')\
    .agg(fsum(col('rent_time')))\
    .withColumn("row_number", row_number().over(w))\
    .where(col('row_number') == 1)\
    .select('city', 'category')\
    .sort('city')\
    .show(1000000)
print('\n')
print('\n')


