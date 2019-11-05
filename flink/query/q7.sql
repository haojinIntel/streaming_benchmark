SELECT
        TUMBLE_START(rowtime, INTERVAL '10' SECOND), TUMBLE_END(rowtime, INTERVAL '10' SECOND), cityId, payProductIds, count(*) num
FROM
        userVisit
WHERE
        payProductIds IS NOT NULL
GROUP BY
        cityId, payProductIds, TUMBLE(rowtime, INTERVAL '10' SECOND)