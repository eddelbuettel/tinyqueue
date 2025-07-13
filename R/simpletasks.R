
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

start_task <- function(x) {
    ## should this consume a task ?
    stopifnot("object 'x' must be 'simpletask' object" = inherits(x, "simpletask"))
    #x$starttime <- Sys.time()
    #x$result <- "started"
    x
}

end_task <- function(x, res) {
    ## should this ack a task ?
    stopifnot("object 'x' must be 'simpletask' object" = inherits(x, "simpletask"))
    #x$endtime <- Sys.time()
    #x$runtime <- x$endtime - x$starttime
    #x$result <- res
    x
}    
