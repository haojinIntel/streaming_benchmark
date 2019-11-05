SELECT
       strategy, site, pos_id, TUMBLE_START(rowtime, INTERVAL '10' SECOND), TUMBLE_END(rowtime, INTERVAL '10' SECOND), count(*) click_count
FROM
       click
GROUP BY
       strategy, site, pos_id, TUMBLE(rowtime, INTERVAL '10' SECOND)