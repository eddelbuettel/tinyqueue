
library(tinyqueue)

cat("--Queue\n")
q <- ensure_queue("tasks")
summary(q)

cat("--Tasks to publish\n")
enqueue_task(q, simpletask("aa", "1.2"))
enqueue_task(q, simpletask("bb", "2.3"))
summary(q)


cat("--Task to consume\n")
st1 <- start_task(q)  # marks 'starting'
st2 <- start_task(q)  # marks 'starting'
summary(q)
Sys.sleep(0.25)
cat("--Task to ack\n")
q <- end_task(q, st1, "skip")
q <- end_task(q, st2, "done")
summary(q)

## cleanup
ignored <- q$con$del("tasks_done")
ignored <- q$con$del("tasks_skip")
ignored <- q$con$del("tasks_todo")
ignored <- q$con$del("tasks_status")
