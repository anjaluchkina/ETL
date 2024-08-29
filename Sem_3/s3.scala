/*
chcp 65001 && spark-shell -i \Users\HYPERPC\Desktop\IT\ETL\Sem_3\s3.scala --conf "spark.driver.extraJavaOptions=-Dfile.encoding=utf-8"
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
        .load("/Users/HYPERPC/Desktop/IT/ETL/Sem_3/s3.xlsx")
		df1.write.format("jdbc").option("url","jdbc:mysql://localhost:3306/spark?user=root&password=anna^2506")
        .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "Sem_3task0")
        .mode("overwrite").save()
	val q = """
	SELECT ID_тикета, FROM_UNIXTIME(StatusTime)StatusTime, 
	(LEAD(StatusTime) OVER(PARTITION BY ID_тикета ORDER BY StatusTime)-StatusTime)/3600 Длительность,
	CASE WHEN Статус IS NULL THEN @PREV1
	ELSE @PREV1:=Статус END
	Статус, 
	CASE WHEN Группа IS NULL THEN @PREV2
	ELSE @PREV2:=Группа END
	Группа,Назначение FROM
	(SELECT ID_тикета, StatusTime, Статус, IF(ROW_NUMBER() OVER(PARTITION BY ID_тикета ORDER BY StatusTime) =1 AND Назначение IS NULL,'', Группа) Группа, Назначение FROM
	(SELECT DISTINCT a.objectid ID_тикета, a.restime StatusTime, Статус, Группа, Назначение, 
	(SELECT @PREV1:=''), (SELECT @PREV2:='') 
	FROM(SELECT DISTINCT objectid, restime FROM spark.sem_3task0
	WHERE fieldname IN ('GNAME2','Status')) a
	LEFT JOIN (SELECT DISTINCT objectid, restime, fieldvalue Статус FROM spark.sem_3task0
	WHERE fieldname IN ('Status')) a1
	ON a.objectid = a1.objectid AND a.restime = a1.restime
	LEFT JOIN (SELECT DISTINCT objectid, restime, fieldvalue Группа, 1 Назначение FROM spark.sem_3task0
	WHERE fieldname IN ('GNAME2')) a2
	ON a.objectid = a2.objectid AND a.restime = a2.restime) b1)b2
	"""
	spark.read.format("jdbc").option("url","jdbc:mysql://localhost:3306/spark?user=root&password=anna^2506")
        .option("driver", "com.mysql.cj.jdbc.Driver").option("query", q)
		.load()
	.write.format("jdbc").option("url","jdbc:mysql://localhost:3306/spark?user=root&password=anna^2506")
        .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "Sem_3task1")
        .mode("overwrite").save()

	 println("task 1")
}
val s0 = (System.currentTimeMillis() - t1)/1000
val s = s0 % 60
val m = (s0/60) % 60
val h = (s0/60/60) % 24
println("%02d:%02d:%02d".format(h, m, s))
System.exit(0)