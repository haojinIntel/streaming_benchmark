SELECT
        sessionId, MAX(TO_UNIX_TIMESTAMP(actionTime, 'yyyy-MM-dd HH:mm:ss')) as timmm , MIN(TO_UNIX_TIMESTAMP(actionTime, 'yyyy-MM-dd HH:mm:ss')) as timmm2, count(*)
FROM
        userVisit
GROUP BY
        sessionId, WINDOW(actionTime, '10 seconds')