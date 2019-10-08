SELECT
       a.dt, a.h, SUM(a.len) total
FROM
       (SELECT
                sessionId, MAX(UNIX_TIMESTAMP(actionTime))-MIN(UNIX_TIMESTAMP(actionTime)) as len, DAYOFMONTH(CAST(actionTime AS TIMESTAMP)) as dt, HOUR(CAST(actionTime AS TIMESTAMP)) as h
        FROM
                userVisit
        GROUP BY
                sessionId, DAYOFMONTH(CAST(actionTime AS TIMESTAMP)), HOUR(CAST(actionTime AS TIMESTAMP))) a " +
GROUP BY
       a.dt, a.h