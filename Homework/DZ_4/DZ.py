import pyspark,time,platform,sys,os
from datetime import datetime
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col,lit,current_timestamp
import pandas as pd
import matplotlib.pyplot as plt 
from sqlalchemy import inspect,create_engine
from pandas.io import sql
import warnings,matplotlib
warnings.filterwarnings("ignore")

con = create_engine("mysql://root:anna^2506^@localhost/spark")

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder.appName("HW").getOrCreate()

sql.execute("""
drop table if exists spark.DZ_4t1""",con)
sql.execute("""
CREATE TABLE if not exists spark.DZ_4t1 (
	`number` INT NULL DEFAULT NULL,
	`Month` DATE NULL DEFAULT NULL,
	`Payment amount` FLOAT NULL DEFAULT NULL,
	`Payment of the principal debt` FLOAT NULL DEFAULT NULL,
	`Payment of interest` FLOAT NULL DEFAULT NULL,
	`Balance of debt` FLOAT NULL DEFAULT NULL,
	`interest` FLOAT NULL DEFAULT NULL,
	`debt` FLOAT NULL DEFAULT NULL
)
COLLATE='utf8mb4_0900_ai_ci'
ENGINE=InnoDB""",con)

from pyspark.sql.window import Window
from pyspark.sql.functions import sum as sum1
w = Window.partitionBy(lit(1)).orderBy("number").rowsBetween(Window.unboundedPreceding, Window.currentRow)
dfG = spark.read.format("com.crealytics.spark.excel")\
        .option("dataAddress", "'General'!A1:F361")\
        .option("useHeader", "false")\
        .option("treatEmptyValuesAsNulls", "false")\
        .option("inferSchema", "true").option("addColorColumns", "true")\
	.option("usePlainNumberFormat","true")\
        .option("startColumn", 0)\
        .option("endColumn", 99)\
        .option("timestampFormat", "MM-dd-yyyy HH:mm:ss")\
        .option("maxRowsInMemory", 20)\
        .option("excerptSize", 10)\
        .option("header", "true")\
        .format("excel")\
        .load("/Users/HYPERPC/Desktop/IT/ETL/Homework/DZ_4/DZ.xlsx").limit(10)\
        .withColumn("interest", sum1(col("Payment of interest")).over(w))\
        .withColumn("debt", sum1(col("Payment of the principal debt")).over(w))

df120 = spark.read.format("com.crealytics.spark.excel")\
        .option("dataAddress", "'120'!A1:F135")\
        .option("useHeader", "false")\
        .option("treatEmptyValuesAsNulls", "false")\
        .option("inferSchema", "true").option("addColorColumns", "true")\
	.option("usePlainNumberFormat","true")\
        .option("startColumn", 0)\
        .option("endColumn", 99)\
        .option("timestampFormat", "MM-dd-yyyy HH:mm:ss")\
        .option("maxRowsInMemory", 20)\
        .option("excerptSize", 10)\
        .option("header", "true")\
        .format("excel")\
        .load("/Users/HYPERPC/Desktop/IT/ETL/Homework/DZ_4/DZ.xlsx").limit(10)\
        .withColumn("interest", sum1(col("Payment of interest")).over(w))\
        .withColumn("debt", sum1(col("Payment of the principal debt")).over(w))

df150 = spark.read.format("com.crealytics.spark.excel")\
        .option("dataAddress", "'150'!A1:F93")\
        .option("useHeader", "false")\
        .option("treatEmptyValuesAsNulls", "false")\
        .option("inferSchema", "true").option("addColorColumns", "true")\
	.option("usePlainNumberFormat","true")\
        .option("startColumn", 0)\
        .option("endColumn", 99)\
        .option("timestampFormat", "MM-dd-yyyy HH:mm:ss")\
        .option("maxRowsInMemory", 20)\
        .option("excerptSize", 10)\
        .option("header", "true")\
        .format("excel")\
        .load("/Users/HYPERPC/Desktop/IT/ETL/Homework/DZ_4/DZ.xlsx").limit(10)\
        .withColumn("interest", sum1(col("Payment of interest")).over(w))\
        .withColumn("debt", sum1(col("Payment of the principal debt")).over(w))

df_combined = dfG.union(df120).union(df150)

df_combined.write.format("jdbc").option("url","jdbc:mysql://localhost:3306/spark?user=root&password=anna^2506^")\
        .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "DZ_4t1")\
        .mode("append").save()

df_pandas1 = dfG.toPandas()
df_pandas2 = df120.toPandas()
df_pandas3 = df150.toPandas()

ax = plt.gca()
ax.ticklabel_format(style='plain')

df_pandas1.plot(kind='line', x='number', y='debt', color='green', ax=ax, label='Долг при платеже 86')
df_pandas1.plot(kind='line', x='number', y='interest', color='red', ax=ax, label='Процент при платеже 86')
df_pandas2.plot(kind='line', x='number', y='debt', color='blue', ax=ax, label='Долг при платеже 120') 
df_pandas2.plot(kind='line', x='number', y='interest', color='orange', ax=ax, label='Процент при платеже 120')
df_pandas3.plot(kind='line', x='number', y='debt', color='purple', ax=ax, label='Долг при платеже 150')  
df_pandas3.plot(kind='line', x='number', y='interest', color='cyan', ax=ax, label='Процент при платеже 150')  


plt.title('Платежи по кредиту')
plt.grid ( True )
ax.set(xlabel=None)

plt.show() 

t0 = time.time()
spark.stop()
t1 = time.time()
print('finished', time.strftime('%H:%M:%S', time.gmtime(round(t1 - t0))))


