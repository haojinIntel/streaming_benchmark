SELECT
        sessionId, MAX(actionTime)-MIN(actionTime) as len
FROM
        userVisit
GROUP BY
        sessionId