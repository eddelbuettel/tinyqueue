##system("docker run -d --rm --name valkey -p 6379:6379 valkey/valkey")
## assume Redis running

library(tinyqueue)

q <- ensure_queue("somejobs")
print(q)

publish(q, message = "Hello world!")
publish(q, message = "Hello again!")
#cat("--Messages after two enqueus\n")
#str(list_messages(q))
summary(q)

msg <- try_consume(q)
#cat("\n--Consumed message 1\n")
#cat("--Messages after consume\n")
#str(list_messages(q))
summary(q)

m <- ack(q, msg)
#cat("--Messages after ack 1\n")
#str(list_messages(q))
summary(q)

msg2 <- try_consume(q)
#cat("--Consumed message 2\n")
m2 <- ack(q, msg2)
#cat("--Messages after ack 2\n")
#str(list_messages(q))
summary(q)

#cat("--Final try consume\n")
msg3 <- try_consume(q)
summary(q)

cleanup(q)

