SELECT
        WINDOW(actionTime, '10 seconds').start start, WINDOW(actionTime, '10 seconds').end finish ,count(*) as sequence
FROM
        userVisit
WHERE
        clickCategoryId IS NOT NULL
GROUP BY
        cityId, WINDOW(actionTime, '10 seconds')