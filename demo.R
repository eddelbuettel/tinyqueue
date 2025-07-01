## Started from RcppRedis::inst/demos/queueDemo.R
##
## Initial version by Iñaki Ucar using Redux
## Adapted to RcppRedis by Dirk Eddelbuettel

useRcppRedis <- function() {

    ensure_queue <- function(name) {
        ##list(con = redux::hiredis(), name = name, temp = paste0(name, "_temp"))
        redis <- new(RcppRedis::Redis)
        list(con = redis,
             name = name,
             todo = paste0(name, "_todo"),
             work = paste0(name, "_work"),
             done = paste0(name, "_done")
             )
    }

    publish <- function(queue, message) {
        ##invisible(queue$con$LPUSH(queue$name, message))
        invisible(queue$con$lpush(queue$todo, message))
    }

    list_messages <- function(queue) {
        ##list(READY = queue$con$LRANGE(queue$name, 0, -1),
        ##     PROCESSING = queue$con$LRANGE(queue$temp, 0, -1))
        list(RECV = queue$con$lrange(queue$todo, 0, -1),
             PROC = queue$con$lrange(queue$work, 0, -1),
             DONE = queue$con$lrange(queue$done, 0, -1))
    }

    try_consume <- function(queue) {
        ##message <- queue$con$RPOPLPUSH(queue$name, queue$temp)
        message <- queue$con$lmove(queue$todo, queue$work, 'RIGHT', 'LEFT')
        if (is.null(message)) return(message)
        list(queue = queue, message = message)
    }

    ack <- function(message) {
        ##invisible(message$queue$con$LREM(message$queue$temp, 1, message$message))
        #invisible(message$queue$con$lrem(message$queue$todo, 1, message$message))
        queue <- message$queue
        msg <- queue$con$lmove(queue$work, queue$done, 'RIGHT', 'LEFT')
        stopifnot("wrong message acknowledged" = all.equal(msg, message$message))
        #cat("'", msg, "' -- '", message, "'\n", sep="")
        #if (is.null(message)) return(message)
        #list(queue = queue, message = message)
        msg
    }

    cleanup <- function(queue) {
        ##invisible(message$queue$con$LREM(message$queue$temp, 1, message$message))
        invisible(queue$con$del(queue$done))
        #queue$con$lrem(queue$done, 2, "*")
    }
    #############################################################################

    ##system("docker run -d --rm --name valkey -p 6379:6379 valkey/valkey")
    ## assume Redis running

    q <- ensure_queue("jobs")
    ##q

    publish(q, message = "Hello world!")
    publish(q, message = "Hello again!")
    cat("\n--Messages after two enqueus\n")
    str(list_messages(q))

    msg <- try_consume(q)
    cat("\n--Consumed message 1\n")
    msg$message
    cat("\n--Messages after consume\n")
    str(list_messages(q))

    print(ack(msg))
    cat("\n--Messages after ack 1\n")
    str(list_messages(q))

    msg2 <- try_consume(q)
    cat("\n--Consumed message 2\n")
    msg2$message
    print(ack(msg2))
    cat("\n--Messages after ack 2\n")
    str(list_messages(q))

    cat("\n--Final try consume\n")
    str(try_consume(q))

    cleanup(q)
    ##system("docker stop valkey")
}


quickCheck <- function() {
    redis <- new(RcppRedis::Redis)
    redis$lpush("foo", "banana")
    redis$lpush("foo", "banana")
    #print(str(redis$lrange("foo", 0, -1)))
    print(redis$llen("foo"))
    redis$lrem("foo", 1, "banana")
    #print(str(redis$lrange("foo", 0, -1)))
    print(redis$llen("foo"))
    redis$lpop("foo")
    #print(str(redis$lrange("foo", 0, -1)))
    print(redis$llen("foo"))
    invisible(NULL)
}

useRcppRedis()
#quickCheck()
