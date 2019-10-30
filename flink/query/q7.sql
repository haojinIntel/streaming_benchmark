SELECT
       a.dt, a.h, SUM(a.len) total
FROM
       (SELECT
                sessionId, MAX(actionTime)-MIN(actionTime) as len, DAYOFMONTH(CAST(actionTime AS TIMESTAMP)) as dt, HOUR(CAST(actionTime AS TIMESTAMP)) as h
        FROM
                userVisit
        GROUP BY
                sessionId, DAYOFMONTH(CAST(actionTime AS TIMESTAMP)), HOUR(CAST(actionTime AS TIMESTAMP))) a
WHERE
       a.len < 1
GROUP BY
       a.dt, a.h
