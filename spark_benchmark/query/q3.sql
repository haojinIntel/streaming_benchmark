SELECT
        strategy, site, pos_id, WINDOW(imp_time, '10 seconds').start, pos_id, WINDOW(imp_time, '10 seconds').end, SUM(cost)
FROM
        imp
GROUP BY
        strategy, site, pos_id, WINDOW(imp_time, '10 seconds')