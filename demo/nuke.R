
library(RcppRedis)

redis <- new(Redis)

keys <- redis$keys("somejobs*")
for (k in keys) {
    cat("Nuking", k, "\n")
    redis$del(k)
}
