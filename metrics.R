library(plyr)
library(parallel)
library(zoo)
library(ggplot2)
library(scales)
library(strucchange)
library(cpm)
library(forecast)
library(xts)
library(cluster)
library(fpc)
library(utils)
library(quantreg)

lsd <- function(pos=1) {
  names(grep("^function$",
             sapply(ls(pos=pos), function(x) {mode(get(x))}),
             value=T,
             invert=T))
}

read.whisper.export <- function(file.name) {
  as.xts(read.zoo(
    file.name,
    na.strings="None",
    colClasses=c("integer", "numeric"),
    col.names=c("time", basename(file.name)),
    FUN=function(t) {as.POSIXct(t, origin="1970-01-01 00:00:00")},
    drop=FALSE))
}

load.metrics <- function(path=".") {
  merge.files(list.files(path, full.names=TRUE))
}

merge.files <- function(files) {
  k <- length(files)
  if (k == 1) {
    read.whisper.export(files[1])
  } else if (k > 1) {
    merge.xts(merge.files(files[1 : (k %/% 2)]),
              merge.files(files[(k %/% 2 + 1) : k]))
  }
}

read.jmeter.csv <- function(file.name) {
  as.xts(read.zoo(
    file.name,
    header=TRUE,
    sep=",",
    ## timeStamp,elapsed,label,responseCode,responseMessage,threadName,success,bytes,grpThreads,allThreads,Latency
    colClasses=c("character", "numeric", "NULL", "NULL", "NULL", "NULL", "logical", "numeric", "integer", "integer", "numeric"),
    FUN=function(t) {as.POSIXct(substr(t, 1, 10), origin="1970-01-01 00:00:00", format='%s')},
    drop=FALSE))
}

get.rates <- function(jmeter, interval.seconds) {
  ticks <- align.time(index(jmeter), interval.seconds)
  success <- as.xts(aggregate(jmeter[,"success"] == 1, ticks, sum)) / interval.seconds
  error <- as.xts(aggregate(jmeter[,"success"] == 0, ticks, sum)) / interval.seconds
  rates <- merge.xts(success, error)
  colnames(rates) <- c("success_rate", "error_rate")
  rates
}

heatmap <- function(metric, bins=500) {
  ggplot(fortify(metric, melt=T), aes(Index, Value)) + stat_bin2d(bins=bins) + scale_fill_gradientn(colours=rainbow(7)) + facet_grid(Series ~ .)
}

set.cores <- function(cores = detectCores()) {
  options(mc.cores = cores)
}

get.correlation.matrix <- function(metrics, complete=0.1, method="spearman", fill=0.1) {
  counts <- sapply(metrics, function(x) {sum(!is.na(x))})
  n <- ncol(metrics)
  l <- nrow(metrics)
  d <- coredata(metrics)
  rl <- mclapply(combn(n, 2, simplify=FALSE), function(ij) {
    i <- ij[1]
    j <- ij[2]
    x2 <- d[,i]
    y2 <- d[,j]
    ok <- complete.cases(x2, y2)
    mc <- max(counts[i], counts[j])
    if (mc != 0 & sum(ok)/mc >= complete & sum(ok)/l >= fill) {
      x2 <- x2[ok]
      y2 <- y2[ok]
      cr <- cor(x2, y2, method=method)
      if (is.na(cr)) {
        cr <- 0
      }
    } else {
      cr <- 0
    }
    list(i, j, cr)
  })
  r <- matrix(0, nrow=n, ncol=n)
  for (ijc in rl) r[ijc[[1]], ijc[[2]]] <- ijc[[3]]
  r <- r + t(r) + diag(n)
  rownames(r) <- colnames(metrics)
  colnames(r) <- colnames(metrics)
  r
}

get.correlation.distance <- function(metrics, complete=0.1, method="spearman", fill=0.1) {
  as.dist(1-abs(get.correlation.matrix(metrics, complete, method, fill)))
}

filter.statsd <- function(metrics) {
  filter.colnames("sum(_50|_90|_99)$|mean(_50|_90|_99)?$|^stats_counts",
                  metrics,
                  TRUE)
}

filter.codahale_like <- function(metrics) {
  metrics <- filter.colnames("\\.acceleration\\.[^\\.]+$|\\.(day|fifteen|five|mean|one)$|\\.(arithmetic_mean|geometric_mean|harmonic_mean|kurtosis|skewness|standard_deviation|variance)$|\\.reductions_since_last_call$|\\.n$|\\.stddev$|MinuteRate$",
                             metrics,
                             TRUE)
  columns <- colnames(metrics)
  counters <- grep("\\.number_of_gcs$|\\.words_reclaimed$|\\.io\\.input$|\\.io\\.output$|\\.total_reductions$|\\.count$|\\.vm\\.context_switches$|\\.runtime\\.total_run_time$",
                   columns, value=TRUE)
  metrics[, counters] <- diff(metrics[, counters, drop=FALSE], na.pad=TRUE)
  metrics
}

filter.metrics <- function(metrics, change.threshold=0.01) {
  cpu.columns <- grep("\\.cpu\\.[[:digit:]]+\\.cpu\\.(softirq|steal|system|user|wait|interrupt)\\.value$",
                      colnames(metrics),
                      value=TRUE)
  cpu.sums <- sapply(
    tapply(cpu.columns,
           sub("^(.*)\\.cpu\\.[[:digit:]]+\\.(.*)$", "\\1.\\2", cpu.columns),
           c),
    function(cl) {
      rowSums(metrics[,cl,drop=FALSE], na.rm=TRUE)
    })
  if (length(cpu.sums) > 0) {
    metrics <- cbind(metrics, cpu.sums)
  }
  metrics <- metrics[,
                     !grepl("df_complex\\.used\\.value$|\\.cpu\\.[[:digit:]]+\\.cpu\\.|\\.disk\\.sd[a-z][0-9]\\.", colnames(metrics)),
                     drop=FALSE]
  columns <- colnames(metrics)
  medians <- apply(metrics, 2, median, na.rm=TRUE)
  ranges <- apply(metrics, 2, range, na.rm=TRUE)
  metrics[,
          (!grepl("\\.cpu\\.[[:alpha:]]+\\.value$", columns) | ranges[2,] > 5)
          & ranges[1,] != ranges[2,]
          & is.finite(ranges[1,])
          & is.finite(ranges[2,])
          & (!grepl("load\\.(longterm|midterm|shortterm)$", columns) | ranges[2,] > 0.5)
          & abs((ranges[2,] - ranges[1,])/medians) > change.threshold
          & (!grepl("if_octets", columns) | ranges[2,] > 1000)
          & (!grepl("if_packets", columns) | ranges[2,] > 10),
          drop=FALSE
          ]
}

get.relative.time <- function(metrics) {
  index(metrics) - min(index(metrics))
}

get.correlation <- function(x, metrics, subset=1:nrow(metrics), complete=0.1, method="spearman") {
  x <- coredata(x[subset])
  nx <- sum(!is.na(x))
  m <- coredata(metrics[subset,])
  simplify2array(mclapply(1:ncol(m), function(k) {
    y <- m[,k]
    ok <- complete.cases(x, y)
    if ((sum(ok) / max(nx, sum(!is.na(y)))) > complete) {
      x2 <- x[ok]
      y2 <- y[ok]
      cr <- abs(cor(x2, y2, method=method))
      if (is.na(cr)) {
        0
      } else {
        cr
      }
    } else {
      0
    }
  }))
}

find.correlated <- function(x, metrics, subset=1:nrow(metrics), complete=0.1, method="spearman") {
  metrics[,
          order(get.correlation(x, metrics, subset, complete, method),
                decreasing=TRUE),
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

filter.any.na <- function(metrics) {
  metrics[,
          which(sapply(metrics, function(v) {all(!is.na(v))})),
          drop=FALSE]
}

find.na <- function(metrics, subset=1:nrow(metrics)) {
  metrics[,
          which(sapply(metrics[subset,], function(v) {all(is.na(v))})),
          drop=FALSE]
}

find.changed.sd <- function(metrics, a, b) {
  sd.a <- sapply(metrics[a, ], sd, na.rm=TRUE)
  sd.b <- sapply(metrics[b, ], sd, na.rm=TRUE)
  metrics[,
          order(sd.b/sd.a, decreasing=TRUE, na.last=TRUE),
          drop=FALSE]
}

find.changed.mean <- function(metrics, a, b) {
  sd.a <- sapply(metrics[a, ], sd, na.rm=TRUE)
  mean.a <- sapply(metrics[a, ], mean, na.rm=TRUE)
  mean.b <- sapply(metrics[b, ], mean, na.rm=TRUE)
  metrics[,
          order(abs(mean.b - mean.a)/sd.a,
                     decreasing=TRUE,
                     na.last=TRUE),
          drop=FALSE]
}

filter.colnames <- function(pattern, metrics, ...) {
  metrics[,
          grep(pattern, colnames(metrics), ...),
          drop=FALSE]
}

multiplot <- function(metrics, limit=50) {
  data <- metrics[,
                  which(sapply(metrics, function(v) {!all(is.na(v))})),
                  drop=FALSE]
  r <- nrow(data)
  n <- ncol(data)
  i <- 1
  k <- min(n, limit)
  repeat {
    m <- data[, i:k, drop=FALSE]
    ms <- sapply(m, function(y) {
      ave(coredata(y),
          seq.int(r) %/% max(3, r %/% 500),
          FUN=function(x) {mean(x, na.rm=T)})
    })
    df <- data.frame(index(m)[rep.int(1:r, ncol(m))],
                     factor(rep(1:ncol(m), each = r), levels = 1:ncol(m), labels = colnames(m)),
                     as.vector(coredata(m)),
                     as.vector(coredata(ms)))
    names(df) <- c("Index", "Series", "Value", "Smooth")
    p <- ggplot(data = df) + geom_point(aes(x = Index, y = Value), na.rm=TRUE, shape=".") + geom_path(aes(x = Index, y = Smooth), na.rm=TRUE, color="blue") + xlab(NULL) + ylab(NULL) + facet_grid(Series ~ ., scales = "free_y") + theme(strip.text.y = element_text(angle=0), axis.text.y = element_blank(), axis.ticks.y = element_blank())
    print(p)
    if (k >= n) {
      break
    } else {
      choice <- readline("empty line to continue, anything else to stop :")
      if (choice == "") {
        i <- i + limit
        k <- min(n, k + limit)
      } else {
        break
      }
    }
  }
}

multiplot.numbers <- function(metrics, limit=15) {
 data <- metrics[,
                  which(sapply(metrics, function(v) {!all(is.na(v))})),
                  drop=FALSE]
  r <- nrow(data)
  n <- ncol(data)
  i <- 1
  k <- min(n, limit)
 repeat {
    m <- data[, i:k, drop=FALSE]
    df <- data.frame(index(m)[rep.int(1:r, ncol(m))],
                     factor(rep(1:ncol(m), each = r), levels = 1:ncol(m), labels = colnames(m)),
                     as.vector(coredata(m)))
    names(df) <- c("Index", "Series", "Value")
    p <- ggplot(data = df) + geom_path(aes(x = Index, y = Value), na.rm=TRUE) + xlab(NULL) + ylab(NULL) + facet_grid(Series ~ ., scales = "free_y") + theme(strip.text.y = element_text(angle=0))
    print(p)
    if (k >= n) {
      break
    } else {
      choice <- readline("empty line to continue, anything else to stop :")
      if (choice == "") {
        i <- i + limit
        k <- min(n, k + limit)
      } else {
        break
      }
    }
  }
}

sameplot <- function(metrics) {
  autoplot(metrics, facet=NULL)
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
  result$name <- as.character(result$name)
  result[order(result$time),]
}

detrend <- function(metrics) {
  columns <- colnames(metrics)
  trend.cols <- grep("\\.memory\\.memory\\.|\\.vmstats\\.memory\\.|\\.vmem\\.vmpage_number\\.",
                     columns, value=TRUE)
  metrics[, trend.cols] <- diff(metrics[, trend.cols, drop=FALSE], na.pad=TRUE)
  metrics[, !grepl("\\.load\\.load\\.", columns), drop=FALSE]
}

get.distribution.change <- function(metric, window.seconds=300, by.seconds=60, p.value = 0.05, fill=50) {
  index.range <- range(index(metric))
  half.window = window.seconds %/% 2
  total.seconds <- as.integer(difftime(index.range[2], index.range[1], units="s"))
  indices <- unique(align.time(index(metric), by.seconds))
  indices <- indices[indices > index.range[1] + half.window & indices < index.range[2] - half.window]
  values <- simplify2array(mclapply(indices, function(i) {
    x <- na.omit(as.vector(coredata(window(metric, start=i-half.window, end=i))))
    ## TODO workaround for window() that includes both start and end,
    ## expects metric to have 1s or larger intervals
    y <- na.omit(as.vector(coredata(window(metric, start=i+1, end=i+half.window))))
    lx <- length(x)
    ly <- length(y)
    if (length(x) >= 1 & length(y) >= 1
        & length(unique(x)) >= fill
        & length(unique(y)) >= fill) {
      ## TODO Cramer-von-Mises instead of Kolmogorov-Smirnov
      t <- ks.test(x, y, exact=FALSE)
      if (t$p.value < p.value)
        t$statistic
      else {
        NA
      }
    } else {
      NA
    }
  }))
  res <- xts(values, indices)
  colnames(res) <- "distribution-change"
  res
}

## TODO broken
find.changed.distribution <- function(metrics, half.width=100, by=10, p.value = 0.05, u.part=0.1, fill=0.1) {
  change <- simplify2array(mclapply(metrics, function(m) {
    max(get.distribution.change(m, half.width=half.width, by=by, p.value=p.value, u.part=u.part, fill=fill), na.rm=TRUE)
  }))
  indices <- order(change, decreasing=TRUE, na.last=TRUE)
  metrics[,
          indices[!is.na(change[indices]) & !is.infinite(change[indices])],
          drop=FALSE]
}

find.nonlinear <- function(metrics, subset=1:nrow(metrics)) {
  diffs <- simplify2array(mclapply(as.ts(metrics[subset,]), ndiffs))
  indices <- order(diffs, decreasing=TRUE)
  metrics[,
          indices[diffs[indices] > 1],
          drop=FALSE]
}

find.autocorrelated <- function(metrics, subset=1:nrow(metrics), lag=100, p.value=0.05) {
  ac <- simplify2array(mclapply(metrics[subset,], function(m) {
    bt <- Box.test(m, lag=lag, type="Ljung-Box")
    c(bt$statistic, bt$p.value)
  }))
  indices <- na.exclude(order(ac[1,], decreasing=TRUE))
  metrics[,
          na.exclude(indices[ac[2,indices] < p.value]),
          drop=FALSE]
}

get.autocorrelation <- function(metrics, subset=1:nrow(metrics), lag) {
  simplify2array(mclapply(metrics[subset,], function(m) {
    cor(m, lag(m, lag), use="na.or.complete")
  }))
}

plot.medoids <- function(metrics, pamobject, limit=50) {
  sorted <- order(pamobject$silinfo$clus.avg.widths * pamobject$clusinfo[, "size"], decreasing=TRUE)
  sorted <- sorted[which(pamobject$clusinfo[sorted, "size"] > 1)]
  data <- metrics[,
                  pamobject$medoids[sorted],
                  drop=FALSE]
  colnames(data) <- paste("[", sorted, "]",
                          pamobject$medoids[sorted],
                          pamobject$clusinfo[sorted, "size"])
  multiplot(data, limit)
}

plot.cluster <- function(metrics, pamobject, id, limit=50) {
  multiplot(
    metrics[,
            names(
              sort(
                pamobject$silinfo$widths[names(which(pamobject$clustering == id)),
                                         "sil_width"],
                       decreasing=TRUE)),
            drop=FALSE],
    limit)
}

par.pam <- function(d, krange) {
  mclapply(krange, function(k) {pam(d, k, diss=TRUE)})
}

mc.period.apply <- function(metrics, INDEX, FUN, ...) {
  do.call("merge.xts", mclapply(metrics, function(m) {
    period.apply(m, INDEX, FUN, ...)
  }))
}

mc.lm <- function(metrics) {
  rel.time <- get.relative.time(metrics)
  n <- names(metrics)
  lms <- mclapply(metrics, function(m) {
    fit <- lm(coredata(m) ~ rel.time, na.action = na.omit)
    list(residuals = residuals(fit), r.squared = summary.lm(fit)$r.squared)
  })
  m.r <- metrics
  coredata(m.r) <- sapply(lms, function(l) {l$residuals})
  list(residuals = m.r, r.squared = sapply(lms, function(l) {l$r.squared}))
}

mc.rq <- function(metrics) {
  rel.time <- get.relative.time(metrics)
  lms <- simplify2array(mclapply(metrics, function(m) {
    rq(coredata(m) ~ rel.time, na.action = na.omit)$residuals
  }))
  m.r <- xts(lms, order.by = index(metrics))
  names(m.r) <- names(metrics)
  m.r
}

decompose.median <- function(metrics, period) {
  half.window <- period %/% 2
  median.window <- half.window * 2 +1
  l <- nrow(metrics)
  ld <- mclapply(metrics, function(m) {
    # switch back to rollaply(median) to allow NAs in data
    trend <- runmed(coredata(m), median.window, endrule="keep")
    trend[1:half.window] <- NA
    trend[(l - half.window):l] <- NA
    season <- coredata(m) - trend
    figure <- numeric(period)
    l <- length(m)
    index <- seq.int(1, l, by = period) - 1
    for (i in 1:period) figure[i] <- median(season[index + i], na.rm = TRUE)
    list(seasonal=rep(figure, l %/% period + 1)[seq_len(l)],
         trend=trend)
  })
  names(ld) <- NULL
  n = ncol(metrics)
  idx = index(metrics)
  nm = names(metrics)
  uld <- unlist(ld, recursive=FALSE)
  t.m <- matrix(unlist(uld[names(uld) == "trend"], use.name = FALSE), ncol = n)
  trend <- xts(t.m, order.by = idx)
  names(trend) <- nm
  s.m <- matrix(unlist(uld[names(uld) == "seasonal"], use.names = FALSE),
                       ncol = n)
  seasonal <- xts(s.m, order.by = idx)
  names(seasonal) <- nm
  list(trend=trend, seasonal=seasonal)
}

non.seasonal.proportion <- function(metrics, decomposed.metrics) {
  sapply(abs(metrics - decomposed.metrics$trend - decomposed.metrics$seasonal), sum, na.rm=T)/sapply(abs(decomposed.metrics$seasonal), sum, na.rm=T)
}
