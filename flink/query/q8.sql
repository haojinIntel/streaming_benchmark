SELECT
        TUMBLE_START(rowtime, INTERVAL '10' SECOND) startTime, TUMBLE_END(rowtime, INTERVAL '10' SECOND) finish, count(clickCategoryId) as sequence
FROM
        userVisit
WHERE
        clickCategoryId IS NOT NULL
GROUP BY
        TUMBLE(rowtime, INTERVAL '10' SECOND)