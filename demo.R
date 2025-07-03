## Started from RcppRedis::inst/demos/queueDemo.R
##
## Initial version by Iñaki Ucar using Redux
## Adapted to RcppRedis by Dirk Eddelbuettel

tinyqueue <- function(x, ...) {
    UseMethod("tinyqueue")
}

tinyqueue.default <- function(name, ...) {
    redis <- new(RcppRedis::Redis)
    structure(list(con = redis,
                   name = name,
                   todo = paste0(name, "_todo"),
                   work = paste0(name, "_work"),
                   done = paste0(name, "_done")
                   ),
              class = "tinyqueue")
}

.tinyqueue_validate <- function(x) {
    stopifnot("Wrong class, expected 'tinyqueue'" = inherits(x, "tinyqueue"),
              "Cannot reach Redis/Valkey server" = x$con$ping() == "PONG")
    x
}

print.tinyqueue <- function(x, ...) {
    .tinyqueue_validate(x)
    cat("<tinyqueue object containing queue '", x$name, "'>\n", sep="")
}

summary.tinyqueue <- function(x, ...) {
    .tinyqueue_validate(x)
    #print(x)
    cat("<tinyqueue containing todo: ", x$con$llen(x$todo), ",",
        " work: ", x$con$llen(x$work), ",",
        " done: ", x$con$llen(x$done),
        ">\n", sep="")
}

useRcppRedis <- function() {

    ensure_queue <- function(name) {
        tinyqueue(name)
    }

    publish <- function(queue, message) {
        .tinyqueue_validate(queue)
        invisible(queue$con$lpush(queue$todo, message))
    }

    list_messages <- function(queue) {
        .tinyqueue_validate(queue)
        list(RECV = queue$con$lrange(queue$todo, 0, -1),
             PROC = queue$con$lrange(queue$work, 0, -1),
             DONE = queue$con$lrange(queue$done, 0, -1))
    }

    try_consume <- function(queue) {
        .tinyqueue_validate(queue)
        message <- queue$con$lmove(queue$todo, queue$work, 'RIGHT', 'LEFT')
        if (is.null(message)) return(message)
        list(queue = queue, message = message)
    }

    ack <- function(message) {
        queue <- message$queue
        .tinyqueue_validate(queue)
        msg <- queue$con$lmove(queue$work, queue$done, 'RIGHT', 'LEFT')
        stopifnot("wrong message acknowledged" = all.equal(msg, message$message))
        msg
    }

    cleanup <- function(queue) {
        .tinyqueue_validate(queue)
        invisible(queue$con$del(queue$done))
    }
    
    #############################################################################

    ##system("docker run -d --rm --name valkey -p 6379:6379 valkey/valkey")
    ## assume Redis running

    q <- ensure_queue("somejobs")
    print(q)
    
    publish(q, message = "Hello world!")
    publish(q, message = "Hello again!")
    cat("\n--Messages after two enqueus\n")
    #str(list_messages(q))
    summary(q)
    
    msg <- try_consume(q)
    cat("\n--Consumed message 1\n")
    cat("Message is: '", msg$message, "'\n", sep="")
    cat("--Messages after consume\n")
    #str(list_messages(q))
    summary(q)

    m <- ack(msg)
    cat("\n--Messages after ack 1\n")
    #str(list_messages(q))
    summary(q)

    msg2 <- try_consume(q)
    cat("\n--Consumed message 2\n")
    cat("Message is: '", msg2$message, "'\n", sep="")
    m2 <- ack(msg2)
    cat("--Messages after ack 2\n")
    #str(list_messages(q))
    summary(q)

    cat("\n--Final try consume\n")
    msg3 <- try_consume(q)
    summary(q)

    cleanup(q)
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
