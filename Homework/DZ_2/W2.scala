/*
chcp 65001 && spark-shell -i \Users\HYPERPC\Desktop\IT\ETL\Homework\DZ_2\W2.scala --conf "spark.driver.extraJavaOptions=-Dfile.encoding=utf-8"
*/
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions.{col, collect_list, concat_ws}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
// Инициализация SparkSession
val spark = SparkSession.builder
  .appName("YourAppName")
  .config("spark.master", "local[*]")
  .getOrCreate()

// Определяем соединение с базой данных
val misqlCon = "jdbc:mysql://localhost:3306/spark?user=root&password=anna^2506"
val driver = "com.mysql.cj.jdbc.Driver" 

val t1 = System.currentTimeMillis()

if (1 == 1) {
  var df = spark.read.option("delimiter", ",")
    .option("header", "true")
    .csv("/Users/HYPERPC/Desktop/IT/ETL/Homework/DZ_2/fifa_s2.csv")

  // Записываем оригинальный DataFrame в базу данных
  df.write.format("jdbc").option("url", misqlCon)
    .option("driver", driver).option("dbtable", "W2")
    .mode("overwrite").save()
  df.show()

  // Подсчет количества null значений
  val columns_null = df.select(df.columns.map(c => count(when(col(c).isNull || col(c) === "" || col(c).isNaN, c)).alias(c)): _*)
  columns_null.show()

  // Удаляем строки с null значениями
  val df2 = df.na.drop()
  df2.write.format("jdbc").option("url", misqlCon)
    .option("driver", driver).option("dbtable", "W2")
    .mode("overwrite").save()
  df2.show()

  // Применяем преобразования к данным
  val df3 = df2
    .withColumn("Name", lower(col("Name")))
    .withColumn("Nationality", lower(col("Nationality")))
    .withColumn("Club", lower(col("Club")))
    .withColumn("Preferred Foot", lower(col("Preferred Foot")))
    .withColumn("Position", lower(col("Position")))
    .dropDuplicates()

  // Добавляем колонку с группами возраста
  val dfWithAgeGroups = df3.withColumn("Age Group",
    when(col("Age") < 20, "до 20")
      .when(col("Age").between(20, 30), "от 20 до 30")
      .when(col("Age").between(31, 36), "от 30 до 36")
      .otherwise("старше 36")
  )

  // Подсчитываем количество футболистов в каждой категории
  val ageGroupCount = dfWithAgeGroups.groupBy("Age Group").count()

  // Выводим результаты в консоль
  println("Количество футболистов в каждой возрастной категории:")
  ageGroupCount.collect().foreach { row =>
    println(s"Возрастная категория: ${row.getString(0)}, Количество: ${row.getLong(1)}")
  }

  // Сохранение DataFrame с возрастными группами в базу данных
  dfWithAgeGroups.write.format("jdbc").option("url", misqlCon)
    .option("driver", driver).option("dbtable", "w2t1b")
    .mode("overwrite").save()
  dfWithAgeGroups.show()
}

val s0 = (System.currentTimeMillis() - t1) / 1000
val s = s0 % 60
val m = (s0 / 60) % 60
val h = (s0 / 60 / 60) % 24
println("%02d:%02d:%02d".format(h, m, s))
System.exit(0)