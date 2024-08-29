SELECT ID_Тикета, Статус, SUM(Длительность) Длительность FROM sem_3task1
GROUP BY 1, 2
ORDER BY 1,2; 
SELECT DISTINCT ID_Тикета, Статус, SUM(Длительность) OVER(PARTITION BY ID_Тикета, Статус) Длительность FROM sem_3task1
ORDER BY 1,2