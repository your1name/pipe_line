# %%
import os
import time
import datetime
import pyspark.sql.functions as sf
# from uuid import *
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import when
from pyspark.sql.functions import col
# from pyspark.sql.types import *
from pyspark.sql.functions import lit
from pyspark import SparkConf, SparkContext
from uuid import UUID
import time_uuid
from pyspark.sql import Row
from pyspark.sql.functions import udf
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.window import Window as W

# %%
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

packages = ["mysql:mysql-connector-java:8.0.30",
"com.datastax.spark:spark-cassandra-connector_2.12:3.1.0"]
packages_str = ','.join(packages)
# .config("spark.jars.packages", packages_str) \
# khởi tạo session
spark = SparkSession.builder \
.config("spark.jars.packages", packages_str) \
.config("spark.cassandra.connection.host", "localhost")\
.config("spark.cassandra.connection.port", "9042").getOrCreate()

    # Retrieve the SparkConf object from the SparkContext
#conf = spark.sparkContext.getConf()
#conf.getAll()

# %%
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

def extract_time(uuid): # convert uuid to datetime 
  my_uuid =  UUID(uuid)
  return time_uuid.TimeUUID(bytes=my_uuid.bytes).get_datetime().strftime("%Y-%m-%d %H:%M:%S") #2022-07-26 06:45:09

def process_df(df): # convert uuuid từ cột creatime thay thế cho cột ts
# Spilt data


  # Cast data String to Int
  df = df.withColumns({'job_id': col('job_id').cast('int'),
                      'bid': col('bid').cast('int'),
                      'group_id': col('group_id').cast('int'),
                      'publisher_id': col('publisher_id').cast('int'),
                      'campaign_id': col('campaign_id').cast('int'),})
  # Filter null data
  df = df.filter(df.job_id.isNotNull()) # nên sử dụng cách này, có tác dụng khi job_id là Int
  # df = df.filter(col('job_id') !='null') # khắc phục tạm vì do lúc lưu data gặp lỗi nên job_id là String

  # Converting function to UDF
  # StringType() is by default hence not required
  extract_timeUDF = udf(lambda x: extract_time(x), StringType())

  df = df.withColumn('ts', extract_timeUDF(col('create_time')))
  
  df = df.select('ts','job_id','custom_track','bid','campaign_id','group_id','publisher_id')

  return df
  # Cách 2: Thay vì dùng UDF có thể collect() cột create-time ra rồi duyệt vòng for đưa vào 1 list mới
  # Cách 3: sử dụng apply(lambda:........)


# %%
def calculating_clicks(df):
    clicks_data = df.filter(col('custom_track') == "click")
    clicks_data = clicks_data.na.fill({'bid':0})
    clicks_data = clicks_data.na.fill({'campaign_id':0})
    clicks_data = clicks_data.na.fill({'group_id':0})
    clicks_data = clicks_data.na.fill({'publisher_id':0})
    clicks_data.registerTempTable('clicks') # Tạo bảng tạm để thực hiện truy vấn spark sql
    clicks_output = spark.sql(""" SELECT  job_id, date(ts) AS dates, hour(ts) AS hours, publisher_id, campaign_id, group_id,
                      avg(bid) AS bid_set, count(*) AS clicks, sum(bid) AS spend_hour
        FROM clicks
        GROUP BY job_id, date(ts) , hour(ts) , publisher_id, campaign_id, group_id """)
    return clicks_output # return a SparkDataFrame


# %%
def calculating_qualified(df):
    qualified_data = df.filter(col('custom_track') == "qualified")
    qualified_data = qualified_data.na.fill({'bid':0})
    qualified_data = qualified_data.na.fill({'campaign_id':0})
    qualified_data = qualified_data.na.fill({'group_id':0})
    qualified_data = qualified_data.na.fill({'publisher_id':0})
    qualified_data.registerTempTable('qualified') # Tạo bảng tạm để thực hiện truy vấn spark sql
    qualified_output = spark.sql(""" SELECT  job_id, date(ts) AS dates, hour(ts) AS hours, publisher_id, campaign_id, group_id,
                      count(*) AS qualified
        FROM qualified
        GROUP BY job_id, date(ts) , hour(ts) ,publisher_id, campaign_id, group_id """)
    return qualified_output


# %%
def process_final_data(clicks_output,qualified_output):
  final_data = clicks_output.join(qualified_output, ['job_id', 'dates', 'hours','publisher_id', 'campaign_id', 'group_id'],'full')
  return final_data


# %%
def final_output(df):
    clicks = calculating_clicks(df)
    qualified = calculating_qualified(df)
    return process_final_data(clicks, qualified)

# %%
def import_to_sql(output):
    final_output = output.select('job_id','dates','hours','publisher_id','campaign_id','group_id','qualified','clicks','bid_set','spend_hour') # xếp lại thứ tự
    
    final_output.write.mode("append")\
    .format("jdbc")\
    .option("url",  "jdbc:mysql://localhost:3306/ware")\
    .option("dbtable", "events")\
    .option("user",  "root")\
    .option("password","123456")\
    .option("driver",'com.mysql.cj.jdbc.Driver')\
    .save()
    
    return print('Data imported successfully')


# %%
def retrieve_mysql_lastest_time(url, driver, user, password):
    sql = """(select max(last_updated_time) from events) A"""
    mysql_time = spark.read.format('jdbc').options(url = url , driver = driver , dbtable = sql, user=user , password = password).load().take(1)[0][0]
    if mysql_time is None:
        time = '1998-01-01 23:59:59'
    else:
        time = mysql_time.strftime("%Y-%m-%d %H:%M:%S")
    return time



# %%
def retrieve_cass_lastest_time():
    temp_df = spark.read.format("org.apache.spark.sql.cassandra").options(table="tracking",keyspace="test").load() # read Data
    cass_time = temp_df.agg({'ts':'max'}).take(1)[0][0]

    return cass_time.strftime("%Y-%m-%d %H:%M:%S")

# %%
def main(mysql_time):
    
    print("-----------------------------")
    print("Retrieve data from Cassandra and process")
    print("-----------------------------")
    df = spark.read.format("org.apache.spark.sql.cassandra").options(table="tracking",keyspace="test").load().where(col('ts') >= mysql_time) # read Data
    
    print("-----------------------------")
    print("process data")
    print("-----------------------------")
    df = process_df(df)

    print("-----------------------------")
    print("process final output")
    print("-----------------------------")
    cass_output = final_output(df)
    
    print("-----------------------------")
    print("Process data successfully")
    print("-----------------------------")

    import_to_sql(cass_output)
    return 'Task successfully'



# %%
url=   "jdbc:mysql://localhost:3306/ware"
driver = "com.mysql.cj.jdbc.Driver"
user = 'root'
password = '123456'

while True:
    timestart = datetime.datetime.now()

    mysql_time = retrieve_mysql_lastest_time(url, driver, user, password)
    print(f'Latest time in MySQL is {mysql_time}')

    cassandra_time = retrieve_cass_lastest_time()
    print(f'Latest time in Cassandra is {cassandra_time}')

    if cassandra_time > mysql_time :
        main(mysql_time)
    else :
        print('No New data found')

    timeend = datetime.datetime.now()
    
    print("Job takes {} seconds to execute".format((timeend - timestart).total_seconds()))

    print("-------------------------------------------")
    time.sleep(30)


