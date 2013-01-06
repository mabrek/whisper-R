library(plyr)
library(caTools)
library(parallel)

read.file <- function(file.name) {
  data <- na.omit(
    read.table(
      file.name,
      na.strings="None",
      colClasses=c("integer", "numeric"),
      col.names=c("time", basename(file.name))))
  if (nrow(data) == 0)
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
  metrics <- Reduce(merge.metrics,
                    lapply(list.files(path, full.names=TRUE), read.file))
  metrics$rel.time <- metrics$time - min(metrics$time)
  metrics[order(metrics$rel.time),]
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

linear.score.vector <- function (x, y, term = 30, ...) {
  missing.y <- missing(y)
  score <- sapply(1:(length(x) - term), function(i) {
    if(missing.y) {
      df <- data.frame(a = 1:term, b = x[i:(i + term - 1)])
    } else {
      df <- data.frame(a = x[i:(i + term - 1)], b = y[i:(i + term - 1)])
    }
    if(length(na.omit(df$b)) > 0) {
      model <- lm(b ~ a, df, na.action=na.omit, ...)
      if (all(resid(model) == 0)) {
        0
      } else {
        1 - summary(model)$adj.r.squared
      }
    } else {
      NA
    }
  })
  c(rep(NA, round(term/2)),
    scale(score, center=TRUE, scale=FALSE)[,1],
    rep(NA, term - round(term/2)))
}   

filter.columns <- function(df, time.axis = "rel.time", outliers.rm = 5) {
  columns <- colnames(df)
  means <- sapply(df, mean, na.rm=TRUE)
  cleaned.df <- as.data.frame(sapply(columns, function(x) {
    v <- df[[x]]
    v[tail(order(abs(df[[x]]-means[x])), outliers.rm)] <- NA
    v
  }))
  ranges <- sapply(cleaned.df, range, na.rm=TRUE)
  columns[columns != time.axis
          & columns != "time"
          & !grepl("upper(_50|_90|_99)$|sum(_50|_90|_99)$|mean(_50|_90|_99)?$|^stats_counts|cpu\\.idle\\.value$|df_complex\\.used\\.value$", columns)
          & (!grepl("cpu\\.(softirq|steal|system|user|wait)\\.value$", columns) | ranges[2,] > 2)
          & ranges[1,] != ranges[2,]
          & (!grepl("load\\.(longterm|midterm|shortterm)$", columns) | ranges[2,] > 0.5)
          ]
}

linear.score <- function (df, time.axis = "rel.time", ...) {
  columns <- filter.columns(df, time.axis)
  lsv <- function(x) {
    linear.score.vector(df[[time.axis]], x, ...)
  }
  scored <- mclapply(df[columns], lsv, mc.allow.recursive = FALSE)
  as.data.frame(scored)
}

find.maximum <- function(x, smooth = 10, n = 5) {
  smoothed <- runmean(x, smooth)
  maximum.loc <- unique(
    c(which(diff(sign(diff(smoothed))) == -2),
      which.max(smoothed)))
  top.maximum.loc <- maximum.loc[head(order(smoothed[maximum.loc], decreasing=TRUE), n=n)]
  top.maximum <- smoothed[top.maximum.loc]
  cbind(top.maximum.loc, top.maximum)
}

compose.maximum <- function(scored, time.axis, ...) {
  rbind.fill(mclapply(colnames(scored), function(x) {
    maximum <- find.maximum(scored[[x]], ...)
    if (length(maximum) == 0) {
      data.frame()
    } else {
      data.frame(name=x, time=time.axis[maximum[,1]], maximum=maximum[,2])
    }
  }))
}

top.maximum <- function(composed, n=50) {
  composed[head(order(composed$maximum, decreasing=TRUE), n=n),]
}
