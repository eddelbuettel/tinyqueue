
#taskresult <- as.factor(c("done", "failed", "skipped", "unset", "started"))
#taskresult <- c("done", "failed", "skipped", "unset", "started")

#' Represent a \code{simpletask} object suitable for queuing
#'
#' A \code{simpletask} objects describes a task, here usually a package name
#' for a package to be tested along with a version number. It can be any R object
#' as the \code{simplequeue} serializes and de-serializes the object apropriately
#'
#' @title simpletask: A Simple Task Object
#' @param pkg character A package name
#' @param ver character A package version number
#' @param ... dots Currently ignored
#' @param x simpletask A task object
#' @seealso tinyqueue
#' @rdname simpletask
#' @export simpletask
simpletask <- function(pkg, ver, ...) {
    UseMethod("simpletask")
}

#' @rdname simpletask
#' @method simpletask default
#' @export
simpletask.default <- function(pkg, ver, ...) {
    structure(list(package = pkg,
                   version = ver),
              class = "simpletask")
              
}

#' @rdname simpletask
#' @method format simpletask 
#' @export
format.simpletask <- function(x, ...) {
    txt <- paste0("<task: package '", x$package,
                  " (", x$version, ")'",
                  ##" in status '",
                  ##x$result, "'",
                  ##" started at ", format(x$starttime),
                  ##" finished at ", format(x$endtime),
                  ##" time ", format(x$runtime),
                  ">")
    txt
}

#' @rdname simpletask
#' @method print simpletask 
#' @export
print.simpletask <- function(x, ...) {
    cat(format(x, ...), "\n", sep="")
}

#' @rdname simpletask
#' @param q tinyqueue A \code{tinyqueue} object
#' @param st object An R object describing the task that is serialized
enqueue_task <- function(q, st) {
    #print(st)
    publish(q, st)
    ## set state ?
}

.make_name <- function(name) paste0(name,"_","status")

#' @rdname simpletask
start_task <- function(q) {
    stopifnot("object 'q' must be 'tinyqueue' object" = inherits(q, "tinyqueue"))
    st <- try_consume(q)  # fetches message
    q$con$hset(.make_name(q$name), st$package, "started")
    #x$starttime <- Sys.time()
    #x$result <- "started"
    st
}

#' @rdname simpletask
#' @param res numeric The result code of the completed task, must be between 0 and 2
end_task <- function(q, st, res) {
    stopifnot("object 'q' must be 'tinyqueue' object" = inherits(q, "tinyqueue"),
              "object 'st' must be 'simpletask' object" = inherits(st, "simpletask"),
              "object 'res' must be character" = inherits(res, "character"),
              "object 'res' must be one of 'done', 'fail', 'skip'" =
                  res %in% c("done", "fail", "skip"))
    map <- c(done = 0, fail = 1, skip = 2)
    ack(q, st, map[[res]])
    q$con$hset(.make_name(q$name), st$package, res)
    #x$endtime <- Sys.time()
    #x$runtime <- x$endtime - x$starttime
    #x$result <- res
    invisible(q)
}    
