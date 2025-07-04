## Started from RcppRedis::inst/demos/queueDemo.R
##
## Initial version by Iñaki Ucar using Redux
## Adapted to RcppRedis by Dirk Eddelbuettel

## TODO: task object
## - package, version, result (of test)
## - start, end, deltat
## - runner id, hostname


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

format.tinyqueue <- function(x, ...) {
    .tinyqueue_validate(x)
    txt <- sprintf("<%s:%d:%d:%d>",
                   x$name,
                   x$con$llen(x$todo),
                   x$con$llen(x$work),
                   x$con$llen(x$done))
    txt
}

summary.tinyqueue <- function(x, ...) {
    .tinyqueue_validate(x)
    cat("<tinyqueue object ", format(x), ">\n", sep="")
}


useRcppRedis <- function() {

    ensure_queue <- function(name) {
        tinyqueue(name)
    }

    publish <- function(queue, message) {
        .tinyqueue_validate(queue)
        cat("Published '", message, "'\n", sep="")
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
        cat("Consumed '", message, "'\n", sep="")
        message
    }

    ack <- function(queue, message) {
        .tinyqueue_validate(queue)
        msg <- queue$con$lmove(queue$work, queue$done, 'RIGHT', 'LEFT')
        cat("Ack'ed '", message, "'\n", sep="")
        stopifnot("wrong message acknowledged" = all.equal(msg, message))
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
