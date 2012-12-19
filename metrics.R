library(foreach)
library(doParallel)
library(plyr)

read.file <- function(file.name) {
  data <- na.omit(
    read.table(
      file.name,
      na.strings="None",
      colClasses=c("integer", "numeric"),
      col.names=c("time", basename(file.name))))
  if (nrow(data) == 0)
    NA
  else if (min(data[[2]]) == max(data[[2]]))
    NA
  else
    data
}

merge.metrics <- function(x,y) {
  if (all(is.na(x)))
    y
  else if(all(is.na(y)))
    x
  else
    join(x, y, by="time", type="full", match="first")
}

load.metrics <- function(path=".") {
  # TODO combine in parallel
  metrics <- foreach(f=list.files(path, full.names=TRUE), .combine=merge.metrics, .packages="plyr") %dopar% {
    read.file(f)
  }
  metrics$rel.time <- metrics$time - min(metrics$time)
  metrics
}

set.cores <- function(cores = detectCores()) {
  registerDoParallel(cores)
  options(mc.cores = cores)
}

correlate <- function(x) {
  correlated <- cor(x, use="pairwise.complete.obs")
  correlated[is.na(correlated)] <- 0
}

distance <- function(correlated) {
  as.dist(1-abs(correlated))
}

linear.score.vector <- function (x, y, term = 30, ...) {
  missing.y = missing(y)
  score <- sapply(1:(length(x) - term), function(i) {
    if(missing.y) {
      df = data.frame(a = 1:term, b = x[i:(i + term - 1)])
    } else {
      df = data.frame(a = x[i:(i + term - 1)], b = y[i:(i + term - 1)])
    }
    if(length(na.omit(df$b)) > 0) {
      1 - summary(lm(b ~ a, df, na.action=na.omit, ...))$adj.r.squared
    } else {
      NA
    }
  })
  c(rep(NA, round(term/2)), score, rep(NA, term - round(term/2)))
}   

linear.score <- function (df, axis = "relTime", ...) {
  columns = colnames(df)[colnames(df) != axis]
  lsv = function(x) {
    linear.score.vector(df[[axis]], x, ...)
  }
  scored = mclapply(df[columns], lsv, mc.allow.recursive = FALSE)
  scored[[axis]] = df[[axis]]
  as.data.frame(scored)
}
