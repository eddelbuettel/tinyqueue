
#taskresult <- as.factor(c("done", "failed", "skipped", "unset", "started"))
#taskresult <- c("done", "failed", "skipped", "unset", "started")

#' @title simpletask: A Simple Task Object
#' @rdname simpletask
#' @export simpletask
simpletask <- function(pkg, ver, ...) {
    UseMethod("simpletask")
}

#' @rdname simpletask
#' @method simpletask default
#' @export
simpletask.default <- function(pkg, ver) {
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

enqueue_task <- function(q, st) {
    #print(st)
    publish(q, st)
    ## set state ?
}

.make_name <- function(name) paste0(name,"_","status")


start_task <- function(q) {
    stopifnot("object 'q' must be 'tinyqueue' object" = inherits(q, "tinyqueue"))
    st <- try_consume(q)  # fetches message
    q$con$hset(.make_name(q$name), st$package, "started")
    #x$starttime <- Sys.time()
    #x$result <- "started"
    st
}

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
