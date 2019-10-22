select
       commodity, count(userId) num, TUMBLE_START(rowtime, INTERVAL '10' SECOND),TUMBLE_END(rowtime, INTERVAL '10' SECOND), UNIX_TIMESTAMP(TUMBLE_START(rowtime, INTERVAL '10' SECOND)) - UNIX_TIMESTAMP(TO_TIMESTAMP(min(times)))
from
       shopping
group by
       TUMBLE(rowtime, INTERVAL '10' SECOND), commodity