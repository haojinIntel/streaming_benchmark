SELECT
        sessionId, MAX(UNIX_TIMESTAMP(actionTime))-MIN(UNIX_TIMESTAMP(actionTime)) as len
FROM
        userVisit
GROUP BY
        sessionId