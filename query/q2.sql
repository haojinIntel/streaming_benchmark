SELECT
       strategy, site, pos_id, TUMBLE_START(rowtime, INTERVAL '60' SECOND), TUMBLE_END(rowtime, INTERVAL '60' SECOND), count(*) click_count
FROM
       click
GROUP BY
       strategy, site, pos_id, TUMBLE(rowtime, INTERVAL '60' SECOND)