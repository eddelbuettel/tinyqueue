
library(tinyqueue)

q <- ensure_queue("tasks")
summary(q)

st <- simpletask("aa", "1.2")
st
publish(q, st)
summary(q)
rm(st)


## TODO would be nice if one of these two changes status of the other
st <- try_consume(q)  # fetches message
st <- start_task(st)  # marks 'starting'
print(st)
summary(q)

Sys.sleep(0.25)
st <- end_task(st, "done")
print(st)

st <- ack(q, st, 2)
summary(q)

## cleanup
ignored <- q$con$del("tasks_skip")
