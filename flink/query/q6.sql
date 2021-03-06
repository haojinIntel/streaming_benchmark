SELECT
      sessionId, (MAX(actionTime)-MIN(actionTime)) as len,  DAYOFMONTH(CAST(actionTime AS TIMESTAMP)) as dt, HOUR(CAST(actionTime AS TIMESTAMP)) as h, COUNT(sessionId) num
FROM
      userVisit
GROUP BY
      sessionId, DAYOFMONTH(CAST(actionTime AS TIMESTAMP)), HOUR(CAST(actionTime AS TIMESTAMP)), TUMBLE(rowtime, INTERVAL '10' SECOND)
HAVING
      (MAX(actionTime)-MIN(actionTime)) < 100