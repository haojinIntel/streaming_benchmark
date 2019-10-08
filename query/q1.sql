select
       commodity, count(userId) num, TUMBLE_START(proctime, INTERVAL '10' SECOND),TUMBLE_END(proctime, INTERVAL '10' SECOND), UNIX_TIMESTAMP(TUMBLE_START(proctime, INTERVAL '10' SECOND)) - UNIX_TIMESTAMP(TO_TIMESTAMP(min(times)))
from
       shopping
group by
       TUMBLE(proctime, INTERVAL '10' SECOND), commodity