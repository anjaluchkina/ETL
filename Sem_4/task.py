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

spark = SparkSession.builder.appName("Hi").getOrCreate()

columns = ["id", "category_id", "rate", "title", "author"]

data = [(1, "3", "5", "Python", "Anna"),
        (2, "4", "5", "SQL", "Anna2"),
        (3, "5", "5", "SQL", "Anna2")]


df = spark.createDataFrame(data, columns)
    # df.show()

if 1 == 11:   
    df.write.format("jdbc") \
            .option("url", "jdbc:mysql://localhost:3306/spark?user=root&password=anna^2506^")\
            .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "Sem_4t1")\
            .mode("overwrite").save()
    df1 = spark.read.format("com.crealytics.spark.excel") \
            .option("sheetName", "Sheet1") \
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
            .load("/Users/HYPERPC/Desktop/IT/ETL/Sem_4/s4.xlsx")\
            .where(col("title")=="news")

    #df1.show()

    df1.write.format("jdbc") \
            .option("url", "jdbc:mysql://localhost:3306/spark?user=root&password=anna^2506^")\
            .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "Sem_4t1")\
            .mode("append").save()

#Задание 2(задача4)
sql.execute("""
drop table if exists spark.sem_4t2""",con)
sql.execute("""
CREATE TABLE if not exists spark.sem_4t2 (
	`№` INT NULL DEFAULT NULL,
	`Месяц` DATETIME NULL DEFAULT NULL,
	`Сумма платежа` FLOAT NULL DEFAULT NULL,
	`Платеж по основному долгу` FLOAT NULL DEFAULT NULL,
	`Платеж по процентам` FLOAT NULL DEFAULT NULL,
	`Остаток долга` FLOAT NULL DEFAULT NULL,
	`проценты` FLOAT NULL DEFAULT NULL,
	`долг` FLOAT NULL DEFAULT NULL
)
COLLATE='utf8mb4_0900_ai_ci'
ENGINE=InnoDB""",con)

from pyspark.sql.window import Window
from pyspark.sql.functions import sum as sum1
w = Window.partitionBy(lit(1)).orderBy("№").rowsBetween(Window.unboundedPreceding, Window.currentRow)
df1 = spark.read.format("com.crealytics.spark.excel")\
        .option("sheetName", "Sheet1")\
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
        .load("/Users/HYPERPC/Desktop/IT/ETL/Sem_4/s4_2.xlsx").limit(1000)\
        .withColumn("проценты", sum1(col("Платеж по процентам")).over(w))\
        .withColumn("долг", sum1(col("Платеж по основному долгу")).over(w))
df1.write.format("jdbc").option("url","jdbc:mysql://localhost:3306/spark?user=root&password=anna^2506^")\
        .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "Sem_4t2")\
        .mode("append").save()


df2 = df1.toPandas()
# Get current axis 
ax = plt.gca()
ax.ticklabel_format(style='plain')
# bar plot
df2.plot(kind='line', 
        x='№', 
        y='долг', 
        color='green', ax=ax)
df2.plot(kind='line', 
        x='№', 
        y='проценты', 
        color='red', ax=ax)
# set the title 
plt.title('Выплаты')
plt.grid ( True )
ax.set(xlabel=None)
# show the plot 
plt.show()

t0 = time.time()
spark.stop()
t1 = time.time()
print('finished', time.strftime('%H:%M:%S', time.gmtime(round(t1 - t0))))
