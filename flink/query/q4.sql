SELECT
        b.device_id, a.strategy, a.site, a.pos_id, count(b.device_id)
FROM
        click a
JOIN
        dau b
ON
        a.device_id = b.device_id AND a.rowtime BETWEEN b.rowtime - INTERVAL '1' second AND b.rowtime + INTERVAL '1' second
GROUP BY
        b.device_id, a.strategy, a.site, a.pos_id, TUMBLE(a.rowtime, INTERVAL '10' SECOND)
