select
       strategy, site, pos_id, WINDOW(click_time, '10 seconds').start, pos_id, WINDOW(click_time, '10 seconds').end, count(*) click_count
from
       click
GROUP BY
       strategy, site, pos_id, WINDOW(click_time, '10 seconds')