SELECT
       b.device_id, a.strategy, a.site, a.pos_id, count(b.device_id)
FROM
        click a
JOIN
        dau b
ON
        a.device_id = b.device_id AND a.click_time BETWEEN b.dau_time - INTERVAL 1 second AND b.dau_time + INTERVAL 1 second
GROUP BY
        b.device_id, a.strategy, a.site, a.pos_id, WINDOW(a.click_time, '10 seconds')
