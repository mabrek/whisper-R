library(plyr)
library(parallel)
library(zoo)

read.file <- function(file.name) {
  read.zoo(
    file.name,
    na.strings="None",
    colClasses=c("integer", "numeric"),
    col.names=c("time", basename(file.name)),
    FUN=function(t) {as.POSIXct(t, origin="1970-01-01 00:00.00", tz="UTC")},
    drop=FALSE)
}

load.metrics <- function(path=".") {
  do.call(merge.zoo, lapply(list.files(path, full.names=TRUE), read.file))
}

set.cores <- function(cores = detectCores()) {
  options(mc.cores = cores)
}

correlate <- function(x) {
  correlated <- cor(x, use="pairwise.complete.obs")
  correlated[is.na(correlated)] <- 0
  correlated
}

get.distance <- function(correlated) {
  as.dist(1-abs(correlated))
}

filter.metrics <- function(metrics, outliers.rm = 5) {
  ranges <- sapply(metrics, function(v) {
    m <- mean(v, na.rm=TRUE)
    v[tail(order(abs(v - m), na.last=FALSE), outliers.rm)] <- NA
    range(v, na.rm=TRUE)
  })
  columns <- colnames(metrics)
  metrics[,!grepl("upper(_50|_90|_99)$|sum(_50|_90|_99)$|mean(_50|_90|_99)?$|^stats_counts|cpu\\.idle\\.value$|df_complex\\.used\\.value$", columns)
          & (!grepl("cpu\\.(softirq|steal|system|user|wait)\\.value$", columns) | ranges[2,] > 2)
          & ranges[1,] != ranges[2,]
          & is.finite(ranges[1,])
          & is.finite(ranges[2,])
          & (!grepl("load\\.(longterm|midterm|shortterm)$", columns) | ranges[2,] > 0.5)
          ]
}

get.relative.time <- function(metrics) {
  index(metrics) - min(index(metrics))
}

find.correlated <- function(x, metrics, subset=1:nrow(metrics)) {
  correlation <- abs(cor(as.numeric(x[subset]), metrics[subset,],
                         use="pairwise.complete.obs"))
  indices <- order(correlation, decreasing=TRUE)
  metrics[, indices[correlation[indices] > 0.9  & !is.na(correlation[indices])]]
}

exclude.columns <- function(what, from) {
  from[,setdiff(colnames(from), colnames(what))]
}

find.constant <- function(metrics, subset=1:nrow(metrics)) {
  ranges <- sapply(metrics[subset,], function(v) {
    range(v, na.rm=TRUE)
  })    
  metrics[,ranges[1,] == ranges[2,]]
}

find.na <- function(metrics, subset=1:nrow(metrics)) {
  metrics[,which(sapply(metrics[subset,], function(v) {all(is.na(v))}))]
}

find.normal <- function(metrics, subset=1:nrow(metrics), p.value=0.1) {
  p.values <- sapply(metrics, function(v) {
    shapiro.test(as.numeric(v[subset]))$p.value
  })
  metrics[,p.values > p.value]
}
