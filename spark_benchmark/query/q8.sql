SELECT
        WINDOW(actionTime, '3600 seconds').start start, WINDOW(actionTime, '3600 seconds').end finish ,count(*) as sequence
FROM
        userVisit
WHERE
        clickCategoryId IS NOT NULL
GROUP BY
        WINDOW(actionTime, '3600 seconds')