SELECT
        DAYOFMONTH(CAST(actionTime AS TIMESTAMP)) as dt, HOUR(CAST(actionTime AS TIMESTAMP)) as h, count(*) as sequence " +
FROM
        userVisit
WHERE
        clickCategoryId IS NOT NULL
GROUP BY
        clickCategoryId, DAYOFMONTH(CAST(actionTime AS TIMESTAMP)), HOUR(CAST(actionTime AS TIMESTAMP))
ORDER BY
        COUNT(*) desc
LIMIT 10