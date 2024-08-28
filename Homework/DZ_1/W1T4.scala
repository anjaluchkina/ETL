/*
chcp 65001 && spark-shell -i \Users\HYPERPC\Desktop\IT\ETL\DZ_1\W1T4.scala --conf "spark.driver.extraJavaOptions=-Dfile.encoding=utf-8"
*/

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

// Создаем сессию Spark
val spark = SparkSession.builder()
  .appName("Excel to JDBC Example")
  .config("spark.driver.extraJavaOptions", "-Dfile.encoding=utf-8")
  .getOrCreate()

// Определяем переменные для подключения к базе данных
val sqlCoun = "jdbc:mysql://localhost:3306/spark?user=root&password=anna^2506"
val driver = "com.mysql.cj.jdbc.Driver" 

val t1 = System.currentTimeMillis()
if (1 == 1) {
  // Чтение данных из Excel
  val df1 = spark.read
    .format("com.crealytics.spark.excel")
    .option("sheetName", "Sheet1")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("/Users/HYPERPC/Desktop/IT/ETL/DZ_1/w1t4.xlsx")

  df1.show()

  // Запись выборки в первую таблицу
  df1.filter(col("Employee_ID").isNotNull)
    .select("Employee_ID", "Job_Code")
    .write
    .format("jdbc")
    .option("url", sqlCoun)
    .option("driver", driver)
    .option("dbtable", "W1T4a")
    .mode("overwrite")
    .save()

  val nf2 = Window.partitionBy(lit(1)).orderBy("id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

  // Добавление id и заполнение пропусков
  val df2 = df1.withColumn("id", monotonicallyIncreasingId())
    .withColumn("Employee_ID", when(col("Employee_ID").isNull, last("Employee_ID", ignoreNulls = true).over(nf2)).otherwise(col("Employee_ID")))
    .withColumn("table", lit("W1T4b"))
	.drop("Job_Code", "Job","id","table")
    .filter(col("table") === "W1T4b")
    .dropDuplicates()

  // Запись в таблицу W1T4b
  df2.write
    .format("jdbc")
    .option("url", sqlCoun)
    .option("driver", driver)
    .option("dbtable", "W1T4b")
    .mode("overwrite")
    .save()

  // Подготавливаем данные для таблицы W1T4c
  val df3 = df1.select("Job_Code", "Job")
    .dropDuplicates()
    .withColumn("table", lit("W1T4c")) // Добавляем колонку "table", чтобы можно было фильтровать по ней в будущем
	.drop("table")

  // Записываем данные в таблицу W1T4c
  df3.write
    .format("jdbc")
    .option("url", sqlCoun)
    .option("driver", driver)
    .option("dbtable", "W1T4c")
    .mode("overwrite")
    .save()

  // Подготовка для записи в W1T4d
  val df4 = df1.withColumn("table", lit("W1T4d"))
    .drop("id", "Employee_ID", "Name", "Job_Code", "Job","table")
    .dropDuplicates()
    .filter(col("table") === "W1T4d")

  // Запись в таблицу W1T4d
  df4.write
    .format("jdbc")
    .option("url", sqlCoun)
    .option("driver", driver)
    .option("dbtable", "W1T4d")
    .mode("overwrite")
    .save()


  println("Данные успешно записаны в таблицы W1T4a, W1T4b, W1T4c и W1T4d.")
}

val s0 = (System.currentTimeMillis() - t1) / 1000
val s = s0 % 60
val m = (s0 / 60) % 60
val h = (s0 / 60 / 60) % 24
println("%02d:%02d:%02d".format(h, m, s))

System.exit(0)