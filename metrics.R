library(plyr)
library(parallel)
library(zoo)
library(ggplot2)
library(scales)
library(strucchange)
library(forecast)
library(xts)
library(cluster)
library(fpc)
library(utils)
library(quantreg)
library(TSclust)
library(multitaper)
library(fastICA)

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
  tree.merge.xts(files, FUN = read.whisper.export)
}

tree.merge.xts <- function(x, FUN = function(m) {m}) {
  k <- length(x)
  if (k == 1) {
    r <- FUN(x[[1]])
    n <- names(x[1])
    if (!is.null(n)) {
      names(r) <- n
    }
    r
  } else if (k > 1) {
    merge.xts(tree.merge.xts(x[1 : (k %/% 2)], FUN = FUN),
              tree.merge.xts(x[(k %/% 2 + 1) : k], FUN = FUN))
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

aggregate.jmeter <- function(jmeter, interval.seconds) {
  ticks <- align.time(index(jmeter), interval.seconds)
  success <- as.xts(aggregate(jmeter[,"success"] == 1, ticks, sum)) / interval.seconds
  error <- as.xts(aggregate(jmeter[,"success"] == 0, ticks, sum)) / interval.seconds
  elapsed.min <- as.xts(aggregate(jmeter[,"elapsed"], ticks, min, na.rm = TRUE))
  elapsed.max <- as.xts(aggregate(jmeter[,"elapsed"], ticks, max, na.rm = TRUE))
  elapsed.median <- as.xts(aggregate(jmeter[,"elapsed"], ticks, median, na.rm = TRUE))
  elapsed.percentile99 <- as.xts(aggregate(jmeter[,"elapsed"], ticks, quantile, probs = c(0.99), na.rm = TRUE))
  aggregated <- merge.xts(success, error, elapsed.min, elapsed.max, elapsed.median, elapsed.percentile99)
  colnames(aggregated) <- c("jmeter.success.rate", "jmeter.error.rate", "jmeter.elapsed.min", "jmeter.elapsed.max", "jmeter.elapsed.median", "jmeter.elapsed.percentile99")
  aggregated
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
                  invert = TRUE)
}

filter.codahale_like <- function(metrics, counter.maxgap=1) {
  metrics <- filter.colnames("\\.acceleration\\.[^\\.]+$|\\.(day|fifteen|five|one)$|\\.(mean|geometric_mean|harmonic_mean|kurtosis|skewness|standard_deviation|variance)$|\\.reductions_since_last_call$|\\.n$|\\.stddev$|MinuteRate$|\\.meanRate$|\\.publish\\.value$|MemtableColumnsCount\\.value$",
                             metrics,
                             invert = TRUE)
  metrics <- metrics[, which(sapply(metrics[], function(v) {any(!is.na(v))})),
                     drop=FALSE]
  columns <- colnames(metrics)
  counters <- grep("\\.number_of_gcs$|\\.words_reclaimed$|\\.io\\.input$|\\.io\\.output$|\\.total_reductions$|\\.count$|\\.vm\\.context_switches$|\\.runtime\\.total_run_time$|\\.jvm\\.gc.*(time|runs)$|\\.(CompletedTasks|TotalBlockedTasks|SpeculativeRetries|MemtableSwitchCount|BloomFilterFalsePositives|confirm|publish_in|publish_out|ack|deliver_get|deliver)\\.value$|_count$|\\.total\\.count$",
                   columns, value=TRUE)
  metrics[1, counters[which(is.na(metrics[1, counters]))]] <- 0
  # TODO positive only diff, don't interpolate on negative jumps
  metrics[, counters] <- diff(na.locf(na.approx(metrics[, counters, drop=FALSE], maxgap=counter.maxgap)), na.pad=TRUE)
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
                     !grepl("df_complex\\.used\\.value$|\\.disk\\.sd[a-z][0-9]\\.", colnames(metrics)),
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

scale.range <- function(metrics) {
  ranges <- apply(metrics, 2, range, na.rm=TRUE)
  mins = matrix(ranges[1,], nrow = nrow(metrics), ncol = ncol(metrics), byrow = TRUE)
  maxs = matrix(ranges[2,], nrow = nrow(metrics), ncol = ncol(metrics), byrow = TRUE)
  (metrics - mins) / (maxs - mins)
}

get.relative.time <- function(metrics) {
  as.numeric(index(metrics) - min(index(metrics)))
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

find.any.na <- function(metrics) {
  metrics[, which(sapply(metrics, function(v) {any(is.na(v))})), drop=FALSE]
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

multiplot <- function(metrics, limit=15, vline=NA) {
 data <- metrics
 if (length(colnames(data)) == 0) {
   colnames(data) <- 1:ncol(data)
 }
 data <- data[,
              which(sapply(data, function(v) {any(!is.na(v))})),
              drop=FALSE]
  r <- nrow(data)
  n <- ncol(data)
  i <- 1
  k <- min(n, limit)
 repeat {
    m <- data[, i:k, drop=FALSE]
    labels <- colnames(m)
    df <- data.frame(index(m)[rep.int(1:r, ncol(m))],
                     factor(rep(1:ncol(m), each = r), levels = 1:ncol(m), labels = labels),
                     as.vector(coredata(m)))
    names(df) <- c("Index", "Series", "Value")
    p <- ggplot(data = df) + geom_path(aes(x = Index, y = Value), na.rm=TRUE) + xlab(NULL) + ylab(NULL) + facet_grid(Series ~ ., scales = "free_y") + theme(strip.text.y = element_text(angle=0))
    if (!is.na(vline)) {
      p <- p + geom_vline(xintercept=as.numeric(index(metrics)[vline]), colour="red")
    }
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

view <- function(pattern, metrics, limit = 15) {
  multiplot(filter.colnames(pattern, metrics, ignore.case = TRUE),
                    limit = limit)
}

sameplot <- function(metrics) {
  autoplot(metrics, facet=NULL)
}

# TODO pass function to compare
multiplot.sorted <- function(metrics, comparison, decreasing=TRUE, ...) {
  sort.order <- order(comparison, decreasing = decreasing)
  data <- metrics[,
                  sort.order,
                  drop=FALSE]
  colnames(data) <- paste("[", sort.order, "]",
                          comparison[sort.order],
                          names(metrics)[sort.order])
  multiplot(data, ...)
}

robust.histogram <- function(x, probs=c(0.01, 0.99), ...) {
  qplot(x = x, xlim = quantile(x, probs, na.rm=TRUE), ...)
}

find.breakpoints <- function(metrics, segment = 0.25) {
  bpl <- mclapply(metrics, function(m) {
    m <- na.omit(m)
    if ((segment < 1 & floor(segment * length(m)) <= 2)
        | segment > 1) {
      data.frame()
    } else {
      bp <- breakpoints(coredata(m) ~ get.relative.time(m), h=segment)$breakpoints
      if (is.na(bp)) {
        data.frame()
      } else {
        data.frame(name=names(m)[1], time=index(m)[bp])
      }
    }
  })
  result <- rbind.fill(bpl)
  result$name <- as.character(result$name)
  result[order(result$time),]
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
  tree.merge.xts(mclapply(metrics, function(m) {
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
    # TODO switch back to rollaply(median) to allow NAs in data
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
  remainder <- abs(metrics - decomposed.metrics$trend - decomposed.metrics$seasonal)
  seasonal <- abs(decomposed.metrics$seasonal)
  sapply(colnames(metrics), function(m) {
    cc <- complete.cases(remainder[,m], seasonal[,m])
    sum(remainder[cc, m], na.rm = TRUE) / sum(seasonal[cc, m], na.rm = TRUE)
  })
}

maybe.deseason <- function(metrics, period, proportion = 0.3) {
  d <- decompose.median(metrics, period)
  nsp <- non.seasonal.proportion(metrics, d)
  seasonal <- metrics[, !is.na(nsp) & (nsp < proportion), drop = FALSE]
  other <- exclude.columns(seasonal, metrics)
  seasonal <- seasonal - d$seasonal[, names(seasonal)]
  names(seasonal) <- paste(names(seasonal), "deseason", sep = ".")
  merge.xts(seasonal, other)
}

find.outliers <- function(metrics, width, q.prob = 0.1, min.score = 5) {
  tree.merge.xts(mclapply(metrics, function(m) {
    rollapply(m, width, fill = NA, align = "right", FUN = function(w) {
      ## TODO check ranges as in filter metrics
      prev <- as.numeric(w[1:width-1])
      l <- last(w)
      if (all(is.na(prev))) {
        if (!is.na(l)) {
          4 # appeared
        } else {
          0 # remained NA
        }
      } else if (is.na(l)) {
        if (all(is.na(w[2:width-1]))) {
          5 # disappeared
        } else {
          NA
        }
      } else {
        q = quantile(prev, probs = c(0, q.prob, 0.5, 1 - q.prob, 1), na.rm = TRUE, type = 1)
        if (q[1] == q[5]) { # was constant
          if (l == q[1]) {
            0 # remained the same
          } else {
            ## TODO change threshold?
            1 # constant changed
          }
        } else { # wasn't constant
          dq = q[4] - q[2]
          r = q[5] - q[1]
          lc = l - q[3]
          if (dq == 0) { # majority ~= median
            if (abs(lc / r) > 1) {
              3 # outside min-max range
            } else {
              0 # inside min-max range
            }
          } else {
            if ((abs(lc / dq) > min.score)
                & (abs(lc / r) > 1)) {
              2 # outside iqr and min-max range
            } else {
              0 # inside iqr or min-max range
            }
          }
        }
      }
    })
  }))
}

sum.xts.rows <- function(metrics) {
  xts(rowSums(metrics, na.rm=T), index(metrics))
}

find.sparse <- function(metrics, fill = 0.1) {
  l = nrow(metrics)
  metrics[,
          which(sapply(metrics, function(v) {(sum(is.na(v)) / l) > fill})),
          drop=FALSE]
}

svd.prepare <- function(metrics) {
  m = metrics[2:nrow(metrics),] # first row is NA for counters
  m = exclude.columns(find.sparse(m), m)
  m = exclude.columns(find.constant(m), m)
  na.approx(m)
}

svd.run <- function(metrics) {
  svd(scale(metrics)) # scale() amplifiers outliers in svd results 
}

svd.u.xts <- function(udv, metrics) {
  xts(udv$u, order.by=index(metrics))
}

# then use multiplot.sorted(metrics, dv[component,])
svd.dv <- function(udv) {
  apply(diag(udv$d) %*% t(udv$v), 2, function(x) {abs(x)/sum(abs(x))})
}

non.zero.columns <- function(metrics) {
  colnames(metrics)[which(colSums(metrics > 0, na.rm=TRUE) > 0)]
}

mc.xts.apply <- function(metrics, FUN, ...) {
  tree.merge.xts(mclapply(metrics, function(m) {
    FUN(m, ...)
  }))
}

# then find outliers to get mean shifts
diff.median <- function(metrics, window) {
  mc.xts.apply(metrics, function(m) {
    diff(rollapply(m, window, fill=NA, align="center", FUN=median, na.rm=T),
         na.pad=TRUE)
  })
}

widen <- function(x, width) {
  lags <- tree.merge.xts(lapply(-width : width, function(n) {
    lx <- lag.xts(x, n)
    lx[is.na(lx)] <- FALSE
    lx
  }))
  apply(lags, 1, sum) > 0
}

# assumes boolean xts input, treats NAs as false
cooccurences <- function(x, metrics, wider = 0) {
  x <- as.vector(widen(x, wider))
  metrics[is.na(metrics)] <- FALSE
  colSums(metrics & x)
}

find.periods <- function(metrics, ...) {
  nfp <- mclapply(metrics, function(m) {
    spec <- spec.mtm(m, plot=F, Ftest=T, ...)
    data.frame(name=names(m)[1], freq=spec$freq, Ftest=spec$mtm$Ftest)
  })
  result <- rbind.fill(nfp)
  result$name <- as.character(result$name)
  result[order(result$Ftest),]
}

# then use multiplot.sorted(metrics, a[component,])
ica.a <- function(ica) {
  apply(ica$A, 2, function(x) {abs(x)/sum(abs(x))})
}
