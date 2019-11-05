SELECT
        a.device_id, a.strategy, a.site, a.pos_id, b.var2, b.var1, count(*)
FROM
        (SELECT device_id, strategy, site, pos_id FROM click) a
JOIN
        (SELECT device_id, FROM_UNIXTIME(CAST(dau_time/1000 AS BIGINT), 'yyyyMMdd') as var1, FROM_UNIXTIME(CAST(dau_time/1000 AS BIGINT), 'HH') as var2 FROM dau) b
ON
         a.device_id = b.device_id
GROUP BY
         a.device_id, a.strategy, a.site, a.pos_id, b.var2, b.var1