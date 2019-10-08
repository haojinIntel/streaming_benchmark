SELECT
       strategy, site, pos_id, FROM_UNIXTIME(CAST(click_time/1000 AS BIGINT),'yyyy-MM-dd'), FROM_UNIXTIME(CAST(click_time/1000 AS BIGINT), 'HH'), count(*) click_count
FROM
       click
GROUP BY
       strategy, site, pos_id, FROM_UNIXTIME(CAST(click_time/1000 AS BIGINT),'yyyy-MM-dd'), FROM_UNIXTIME(CAST(click_time/1000 AS BIGINT), 'HH')