select
       commodity, count(userId) num, WINDOW(times, '10 seconds').start, WINDOW(times, '10 seconds').end
from
       shopping
group BY
       WINDOW(times, '10 seconds'), commodity