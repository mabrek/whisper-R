library(plyr)
library(parallel)
library(zoo)
library(ggplot2)
library(scales)
library(strucchange)
library(cpm)
library(forecast)

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

# TODO specify outliers.rm as proportion of values
# TODO profile and speed up
filter.metrics <- function(metrics, outliers.rm = 5, change.threshold=0.01) {
  cpu.columns <- grep("\\.cpu\\.[[:digit:]]+\\.cpu\\.(softirq|steal|system|user|wait)\\.value$",
                      colnames(metrics),
                      value=TRUE)
  cpu.sums <- sapply(
    tapply(cpu.columns,
           sub("^(.*)\\.cpu\\.[[:digit:]]+\\.(.*)$", "\\1.\\2", cpu.columns),
           c),
    function(cl) {
      rowSums(metrics[,cl,drop=FALSE])
    })
  if (length(cpu.sums) > 0) {
    metrics <- cbind(metrics, cpu.sums)
  }
  metrics <- metrics[,
                     !grepl("upper(_50|_90|_99)$|sum(_50|_90|_99)$|mean(_50|_90|_99)?$|^stats_counts|df_complex\\.used\\.value$|\\.cpu\\.[[:digit:]]+\\.cpu\\.", colnames(metrics)),
                     drop=FALSE]
  columns <- colnames(metrics)
  medians <- sapply(metrics, median, na.rm=TRUE)
  means <- sapply(metrics, mean, na.rm=TRUE)
  sds <- sapply(metrics, sd, na.rm=TRUE)
  ranges <- sapply(columns, function(n) {
    v <- metrics[,n]
    v[tail(order(abs(v - medians[n]), na.last=FALSE), outliers.rm)] <- NA
    range(v, na.rm=TRUE)
  })
  metrics[,
          (!grepl("\\.cpu\\.[[:alpha:]]+\\.value$", columns) | ranges[2,] > 5)
          & ranges[1,] != ranges[2,]
          & is.finite(ranges[1,])
          & is.finite(ranges[2,])
          & (!grepl("load\\.(longterm|midterm|shortterm)$", columns) | ranges[2,] > 0.5)
          & abs(sds/means) > change.threshold
          & (!grepl("if_octets", columns) | ranges[2,] > 1000)
          & (!grepl("if_packets", columns) | ranges[2,] > 10),
          drop=FALSE
          ]
}

get.relative.time <- function(metrics) {
  index(metrics) - min(index(metrics))
}

find.correlated <- function(x, metrics, subset=1:nrow(metrics), threshold=0.9) {
  correlation <- abs(cor(as.numeric(x[subset]), metrics[subset,],
                         use="pairwise.complete.obs"))
  indices <- order(correlation, decreasing=TRUE)
  metrics[,
          indices[correlation[indices] > threshold
                  & !is.na(correlation[indices])],
          drop=FALSE]
}

exclude.columns <- function(what, from) {
  from[,
       setdiff(colnames(from), colnames(what)),
       drop=FALSE]
}

find.constant <- function(metrics, subset=1:nrow(metrics)) {
  ranges <- sapply(metrics[subset,], function(v) {
    range(v, na.rm=TRUE)
  })    
  metrics[,
          ranges[1,] == ranges[2,],
          drop=FALSE]
}

find.na <- function(metrics, subset=1:nrow(metrics)) {
  metrics[,
          which(sapply(metrics[subset,], function(v) {all(is.na(v))})),
          drop=FALSE]
}

find.changed.sd <- function(metrics, a, b, n=50) {
  sd.a <- sapply(metrics[a, ], sd, na.rm=TRUE)
  sd.b <- sapply(metrics[b, ], sd, na.rm=TRUE)
  metrics[,
          head(order(sd.b/sd.a, decreasing=TRUE, na.last=TRUE), n),
          drop=FALSE]
}

find.changed.mean <- function(metrics, a, b, n=50) {
  sd.a <- sapply(metrics[a, ], sd, na.rm=TRUE)
  mean.a <- sapply(metrics[a, ], mean, na.rm=TRUE)
  mean.b <- sapply(metrics[b, ], mean, na.rm=TRUE)
  metrics[,
          head(order(abs(mean.b - mean.a)/sd.a,
                     decreasing=TRUE,
                     na.last=TRUE),
               n),
          drop=FALSE]
}

filter.colnames <- function(metrics, pattern, invert=FALSE) {
  metrics[,
          grep(pattern, colnames(metrics), invert=invert),
          drop=FALSE]
}

multiplot <- function(metrics) {
  ggplot(aes(x = Index, y = Value),
         data = fortify(metrics, melt = TRUE)) + geom_line() + xlab("") + ylab("") + facet_grid(Series ~ ., scales = "free_y") + theme(strip.text.y = element_text(angle=0), axis.text.y = element_blank(), axis.ticks.y = element_blank())
}

find.breakpoints <- function(metrics, segment = 0.25) {
  rel.time <- get.relative.time(metrics)
  ind <- index(metrics)
  df <- as.data.frame(metrics)
  bpl <- mclapply(colnames(metrics), function(v) {
    if ((segment < 1 & floor(segment * length(na.omit(df[,v]))) <= 2)
        | segment > 1) {
      data.frame()
    } else {
      bp <- breakpoints(df[,v] ~ rel.time, h=segment)$breakpoints
      if (is.na(bp)) {
        data.frame()
      } else {
        data.frame(name=v, time=ind[bp])
      }
    }
  })
  result <- rbind.fill(bpl)
  result[order(result$time),]
}

detrend <- function(metrics) {
  columns <- colnames(metrics)
  trend.cols <- grep("\\.memory\\.memory\\.|\\.vmstats\\.memory\\.|\\.vmem\\.vmpage_number\\.",
                     columns, value=TRUE)
  metrics[, trend.cols] <- diff(metrics[, trend.cols, drop=FALSE], na.pad=TRUE)
  metrics[, !grepl("\\.load\\.load\\.", columns), drop=FALSE]
}

find.distribution.change <- function(metrics, cpmType="Cramer-von-Mises", ARL0=50000, startup=50) {
  cpl <- mclapply(colnames(metrics), function(n) {
    m <- na.omit(metrics[,n])
    cp <- processStream(coredata(m), cpmType, ARL0, startup)$changePoints
    if (length(cp) > 0) {
      data.frame(name=n, time=index(m)[cp])
    } else {
      data.frame()
    }
  })
  result <- rbind.fill(cpl)
  if(!is.null(result)) {
    result[order(result[,"time",drop=FALSE]),]
  } else {
    result
  }
}

find.nonlinear <- function(metrics, subset=1:nrow(metrics)) {
  diffs <- simplify2array(mclapply(metrics[subset,], ndiffs))
  indices <- order(diffs, decreasing=TRUE)
  metrics[,
          indices[diffs[indices] > 1],
          drop=FALSE]
}
