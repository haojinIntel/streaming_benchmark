SELECT
      sessionId, MAX(TO_UNIX_TIMESTAMP(actionTime, 'yyyy-MM-dd HH:mm:ss'))-MIN(TO_UNIX_TIMESTAMP(actionTime, 'yyyy-MM-dd HH:mm:ss')) as len,  DAYOFMONTH(CAST(actionTime AS TIMESTAMP)) as dt, HOUR(CAST(actionTime AS TIMESTAMP)) as h, sum(MAX(TO_UNIX_TIMESTAMP(actionTime, 'yyyy-MM-dd HH:mm:ss'))-MIN(TO_UNIX_TIMESTAMP(actionTime, 'yyyy-MM-dd HH:mm:ss'))) num
FROM
      userVisit
GROUP BY
      sessionId, DAYOFMONTH(CAST(actionTime AS TIMESTAMP)), HOUR(CAST(actionTime AS TIMESTAMP)), WINDOW(actionTime, '10 seconds')