SELECT
        TUMBLE_START(rowtime, INTERVAL '3600' SECOND) start, TUMBLE_END(rowtime, INTERVAL '3600' SECOND) finish, count(clickCategoryId) as sequence
FROM
        userVisit
WHERE
        clickCategoryId IS NOT NULL
GROUP BY
        TUMBLE(rowtime, INTERVAL '3600' SECOND)