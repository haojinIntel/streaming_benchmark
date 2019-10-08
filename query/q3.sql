SELECT
        strategy, site, pos_id, FROM_UNIXTIME(CAST(imp_time /1000 AS BIGINT),'yyyy-MM-dd') as dt, FROM_UNIXTIME(CAST(imp_time AS BIGINT), 'HH') as h, SUM(cost)
FROM
        imp
GROUP BY
        strategy, site, pos_id, FROM_UNIXTIME(CAST(imp_time / 1000 AS BIGINT),'yyyy-MM-dd') , FROM_UNIXTIME(CAST(imp_time AS BIGINT), 'HH')