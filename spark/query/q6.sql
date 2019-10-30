SELECT
      sessionId, MAX(TO_UNIX_TIMESTAMP(actionTime, 'yyyy-MM-dd HH:mm:ss'))-MIN(TO_UNIX_TIMESTAMP(actionTime, 'yyyy-MM-dd HH:mm:ss')) as len,  DAYOFMONTH(CAST(actionTime AS TIMESTAMP)) as dt, HOUR(CAST(actionTime AS TIMESTAMP)) as h, COUNT(sessionId) num
FROM
      userVisit
GROUP BY
      sessionId, DAYOFMONTH(CAST(actionTime AS TIMESTAMP)), HOUR(CAST(actionTime AS TIMESTAMP)), WINDOW(actionTime, '10 seconds')
HAVING
      (MAX(TO_UNIX_TIMESTAMP(actionTime, 'yyyy-MM-dd HH:mm:ss'))-MIN(TO_UNIX_TIMESTAMP(actionTime, 'yyyy-MM-dd HH:mm:ss'))) < 100