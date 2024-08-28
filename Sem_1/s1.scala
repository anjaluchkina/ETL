/*
chcp 65001 && spark-shell -i \Users\HYPERPC\Desktop\IT\ETL\s1.scala --conf "spark.driver.extraJavaOptions=-Dfile.encoding=utf-8"
*/
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions.{col, collect_list, concat_ws}
import org.apache.spark.sql.{DataFrame, SparkSession}
val t1 = System.currentTimeMillis()
if(1==1){
var df1 = spark.read.format("com.crealytics.spark.excel")
        .option("sheetName", "Sheet1")
        .option("useHeader", "false")
        .option("treatEmptyValuesAsNulls", "false")
        .option("inferSchema", "true").option("addColorColumns", "true")
		.option("usePlainNumberFormat","true")
        .option("startColumn", 0)
        .option("endColumn", 99)
        .option("timestampFormat", "MM-dd-yyyy HH:mm:ss")
        .option("maxRowsInMemory", 20)
        .option("excerptSize", 10)
        .option("header", "true")
        .format("excel")
        .load("/Users/HYPERPC/Desktop/IT/ETL/Сем1.xlsx")
		df1.show()
		df1.filter(df1("Код предмета").isNotNull).select("Код предмета","Предмет","Учитель")
		.write.format("jdbc").option("url","jdbc:mysql://localhost:3306/spark?user=root&password=anna^2506")
        .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "tasketl1a")
        .mode("overwrite").save()
		import org.apache.spark.sql.expressions.Window
		val window1 = Window.partitionBy(lit(1)).orderBy(("id")).rowsBetween(Window.unboundedPreceding, Window.currentRow)
		var df2 = df1.withColumn("id",monotonicallyIncreasingId)
		.withColumn("Код предмета", when(col("Код предмета").isNull, last("Код предмета", ignoreNulls = true).over(window1)).otherwise(col("Код предмета")))
		.orderBy("id").drop("id","Предмет","Учитель")
		df2.write.format("jdbc").option("url","jdbc:mysql://localhost:3306/spark?user=root&password=anna^2506")
        .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "tasketl1b")
        .mode("overwrite").save()
		df2.drop("Фамилия студента","Имя студента").write.format("jdbc").option("url","jdbc:mysql://localhost:3306/spark?user=root&password=anna^2506")
        .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "tasketl1c")
        .mode("overwrite").save()
		df1.select("Фамилия студента","Имя студента","Код студента").distinct().write.format("jdbc").option("url","jdbc:mysql://localhost:3306/spark?user=root&password=anna^2506")
        .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "tasketl1d")
        .mode("overwrite").save()
	println("task 1")
}
val s0 = (System.currentTimeMillis() - t1)/1000
val s = s0 % 60
val m = (s0/60) % 60
val h = (s0/60/60) % 24
println("%02d:%02d:%02d".format(h, m, s))
System.exit(0)