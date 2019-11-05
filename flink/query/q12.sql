
SELECT
        *
FROM
    (SELECT
            *, ROW_NUMBER() OVER (PARTITION BY w.cityId ORDER BY w.num DESC) as rownum
     FROM
            (SELECT
                    TUMBLE_START(rowtime, INTERVAL '10' SECOND), TUMBLE_END(rowtime, INTERVAL '10' SECOND), cityId, payProductIds, count(*) num
             FROM
                    userVisit
             WHERE
                    payProductIds IS NOT NULL
             GROUP BY
                    cityId, payProductIds, TUMBLE(rowtime, INTERVAL '10' SECOND)
            ) w
    ) v
WHERE
    v.rownum <= 10