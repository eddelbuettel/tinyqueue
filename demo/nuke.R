
library(RcppRedis)

redis <- new(Redis)

keys <- redis$keys("*")
for (k in keys) {
    cat("Nuking", k, "\n")
    redis$del(k)
}
