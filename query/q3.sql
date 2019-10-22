SELECT
        strategy, site, pos_id, TUMBLE_START(rowtime, INTERVAL '10' SECOND), TUMBLE_END(rowtime, INTERVAL '10' SECOND), SUM(cost)
FROM
        imp
GROUP BY
        strategy, site, pos_id, TUMBLE(rowtime, INTERVAL '10' SECOND)