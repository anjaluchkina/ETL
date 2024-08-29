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

