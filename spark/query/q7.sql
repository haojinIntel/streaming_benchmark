SELECT
        WINDOW(actionTime, '10 seconds').start starts, WINDOW(actionTime, '10 seconds').end finish , cityId, payProductIds, count(*)
FROM
        userVisit
WHERE
        payProductIds IS NOT NULL
GROUP BY
        cityId, payProductIds, WINDOW(actionTime, '10 seconds')