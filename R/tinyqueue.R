## Started from RcppRedis::inst/demos/queueDemo.R
##
## Initial version by Iñaki Ucar using Redux
## Adapted to RcppRedis by Dirk Eddelbuettel

## TODO: task object
## - package, version, result (of test)
## - start, end, deltat
## - runner id, hostname

#' Create a \code{tinyqueue} object
#'
#' A \code{tinyqueue} object manages a job queue for a given and named task,
#' using either \pkg{Redis} or \pkg{Valkey} as a backend. The (basic)
#' functionality is modelled after the \pkg{liteq} package.
#'
#' A \code{tinyqueue} is created and returned by function \code{ensure_queue}.
#' The function \code{publish_queue}, \code{try_consume} and \code{ack} serve
#' to, respectively, enqueue a task, dequeue it in order to do the designated
#' work and then acknowledge completion.  \code{list_message} reports the
#' status, and \code{cleanup} finalize and cleans up.
#'
#' The \code{tinyqueue} object has several associated methods to print,
#' summarize, or format the object.
#'
#' The functionality takes advantage of package \pkg{RcppRedis} and its
#' ability to process arbitrary R objects. This allows task descriptions
#' to be \code{data.frame} or \code{list} objects as needed by the application.
#' For the same reason, no \sQuote{schema} or alike has to be created as the
#' queued task objects are self-sufficient as R objects.
#'
#' @title tinyqueue: A Simple Workqueue Object
#' @param name character The task name for 
#' @param ... other arguments 
#' @examples
#' # This requires Redis or Valkey to run
#' \dontrun{tinyqueue("sometask")}
#'
#' @rdname tinyqueue
#' @export tinyqueue
#' @import RcppRedis
#' @importFrom methods new
#' @exportPattern "^[[:alpha:]]+"
tinyqueue <- function(name, ...) {
    UseMethod("tinyqueue")
}

#' @rdname tinyqueue
#' @method tinyqueue default
#' @export
tinyqueue.default <- function(name, ...) {
    redis <- new(RcppRedis::Redis)
    structure(list(con = redis,
                   name = name,
                   todo = paste0(name, "_todo"),
                   work = paste0(name, "_work"),
                   done = paste0(name, "_done"),
                   fail = paste0(name, "_fail"),
                   skip = paste0(name, "_skip")
                   ),
              class = "tinyqueue")
}

.tinyqueue_validate <- function(x) {
    stopifnot("Wrong class, expected 'tinyqueue'" = inherits(x, "tinyqueue"),
              "Cannot reach Redis/Valkey server" = x$con$ping() == "PONG")
    x
}

#' @param x tinyqueue A \code{tinyqueue} object task name for 
#' @rdname tinyqueue
#' @method print tinyqueue 
#' @export
print.tinyqueue <- function(x, ...) {
    .tinyqueue_validate(x)
    cat("<tinyqueue object containing queue '", x$name, "'>\n", sep="")
}

#' @rdname tinyqueue
#' @method format tinyqueue 
#' @export
format.tinyqueue <- function(x, ...) {
    .tinyqueue_validate(x)
    txt <- sprintf("<%s:%d:%d:%d:%d:%d>",
                   x$name,
                   x$con$llen(x$todo),
                   x$con$llen(x$work),
                   x$con$llen(x$done),
                   x$con$llen(x$fail),
                   x$con$llen(x$skip)
                   )
    txt
}

#' @rdname tinyqueue
#' @method summary tinyqueue 
#' @export
summary.tinyqueue <- function(x, ...) {
    .tinyqueue_validate(x)
    cat("<tinyqueue object ", format(x), ">\n", sep="")
}


#' @rdname tinyqueue
#' @export
ensure_queue <- function(name) {
    tinyqueue(name)
}

#' @param queue tinyqueue A \code{tinyqueue} object task name for 
#' @param message object A R object describing a task that is serialized
#' @rdname tinyqueue
#' @export
publish <- function(queue, message) {
    .tinyqueue_validate(queue)
    cat("Published '", format(message), "'\n", sep="")
    invisible(queue$con$lpush(queue$todo, message))
}

#' @rdname tinyqueue
#' @export
list_messages <- function(queue) {
    .tinyqueue_validate(queue)
    list(todo = queue$con$lrange(queue$todo, 0, -1),
         work = queue$con$lrange(queue$work, 0, -1),
         done = queue$con$lrange(queue$done, 0, -1),
         fail = queue$con$lrange(queue$fail, 0, -1),
         skip = queue$con$lrange(queue$skip, 0, -1))
}

#' @rdname tinyqueue
#' @export
try_consume <- function(queue) {
    .tinyqueue_validate(queue)
    message <- queue$con$lmove(queue$todo, queue$work, 'RIGHT', 'LEFT')
    if (is.null(message)) return(message)
    cat("Consumed '", format(message), "'\n", sep="")
    message
}

#' @rdname tinyqueue
#' @export
ack <- function(queue, message, res=0, check=TRUE) {
    .tinyqueue_validate(queue)
    stopifnot("Argument 'res' must be numeric" = is.numeric(res),
              "Argument 'res' must be between 0 and 2" = res >= 0 & res <= 2)
    tgt <- switch(res + 1,
                  queue$done,   # res == 0 aka success  
                  queue$fail,   # res == 1 aka fail
                  queue$skip)   # res == 2 aka skip
    msg <- queue$con$lmove(queue$work, tgt, 'RIGHT', 'LEFT')
    cat("Ack'ed '", format(message), "'\n", sep="")
    if (check) stopifnot("wrong message acknowledged" = all.equal(msg, message))
    msg
}

#' @rdname tinyqueue
#' @export
cleanup <- function(queue) {
    .tinyqueue_validate(queue)
    invisible(queue$con$del(queue$done))
}
    
