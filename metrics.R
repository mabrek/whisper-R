library(parallel)
library(zoo)
library(ggplot2)
library(scales)
library(strucchange)
library(forecast)
library(xts)
library(cluster)
library(utils)
library(quantreg)
library(TSclust)
library(multitaper)
library(fastICA)
library(tsne)
library(shiny)
library(dygraphs)
library(stringr)
library(bit64)
library(dplyr)

lsd <- function(pos = 1) {
  names(grep("^function$", 
             sapply(ls(pos = pos), function(x) {mode(get(x))}),
             value = T, 
             invert = T))
}

read_whisper_export <- function(file.name) {
  as.xts(
    read.zoo(
      file.name,
      na.strings = "None",
      colClasses = c("integer", "numeric"), 
      col.names = c("time", basename(file.name)),
      FUN = function(t) {as.POSIXct(t, origin = "1970-01-01 00:00:00")}, 
      drop = FALSE))
}

load_metrics <- function(path = ".") {
  merge_files(list.files(path, full.names = TRUE))
}

merge_files <- function(files) {
  tree_merge_xts(files, FUN = read_whisper_export)
}

tree_merge_xts <- function(x, FUN = function(m) {m}) {
  k <- length(x)
  if (k == 1) {
    r <- FUN(x[[1]])
    n <- names(x[1])
    if (!is.null(n)) {
      names(r) <- n
    }
    r
  } else if (k > 1) {
    merge.xts(tree_merge_xts(x[1:(k %/% 2)], FUN = FUN), 
              tree_merge_xts(x[(k %/% 2 + 1):k], FUN = FUN))
  }
}

read_jmeter_csv <- function(file.name) {
  m <- read.table(
    file.name,
    header = TRUE,
    sep = ",",
    # timeStamp,elapsed,label,responseCode,responseMessage,threadName,
    # success,bytes,grpThreads,allThreads,Latency,IdleTime,Connect
    colClasses = c("character", "numeric", "factor", "factor", "NULL", "NULL",
      "logical", "numeric", "integer", "integer", "numeric", "numeric",
      "numeric"))
  m[, "timeStamp.raw"] <- m[, "timeStamp"]
  m[, "timeStamp"] <- as.POSIXct(substr(m[, "timeStamp"], 1, 10),
                                 origin = "1970-01-01 00:00:00",
                                 format = "%s")
  m
}

aggregate_jmeter <- function(jmeter, interval.seconds) {
  columns <- setdiff(colnames(jmeter),
                     c("label", "responseCode", "timeStamp.raw"))
  tree_merge_xts(
    mclapply(
      c(levels(jmeter[, "label"]), ""),
      function(l) {
        if (l == "") {
          prefix <- "jmeter.total"
          idx <- 1:nrow(jmeter)
        } else {
          prefix <- paste("jmeter", l, sep = ".")
          idx <- jmeter[, "label"] == l
        }
        z <- read.zoo(jmeter[idx, columns])
        ticks <- align.time(index(z), interval.seconds)
        success <- as.xts(aggregate(z[, "success"] == 1, ticks, sum)) / interval.seconds
        error <- as.xts(aggregate(z[, "success"] == 0, ticks, sum)) / interval.seconds
        elapsed.raw <- z[, "elapsed"] * 1000  # convert to microseconds
        elapsed.min <- as.xts(aggregate(elapsed.raw, ticks, min, na.rm = TRUE))
        elapsed.max <- as.xts(aggregate(elapsed.raw, ticks, max, na.rm = TRUE))
        elapsed.median <- as.xts(aggregate(elapsed.raw, ticks, median,
                                           na.rm = TRUE))
        elapsed.percentile99 <- as.xts(aggregate(elapsed.raw, ticks, quantile,
                                                 probs = c(0.99), na.rm = TRUE))
        threads.median <- as.xts(aggregate(z[, "grpThreads"], ticks, median,
                                           na.rm = TRUE))
        start.time <- as.integer64(jmeter[idx, "timeStamp.raw"]) +
          jmeter[idx, "Connect"]
        end.time <- as.integer64(jmeter[idx, "timeStamp.raw"]) +
          jmeter[idx, "elapsed"]
        stamps.raw <- c(start.time, end.time)
        stamps.order <- order(stamps.raw)
        concurrency.raw <- zoo(
          cumsum(c(rep_len(1, length(start.time)),
                   rep_len(-1, length(end.time)))[stamps.order]),
          as.POSIXct(stamps.raw[stamps.order] / 1000,
                     origin = "1970-01-01 00:00:00"))
        concurrency.min <- as.xts(aggregate(concurrency.raw,
                                            align.time(index(concurrency.raw),
                                                       interval.seconds),
                                            min, na.rm = TRUE))
        concurrency.max <- as.xts(aggregate(concurrency.raw,
                                            align.time(index(concurrency.raw), 
                                                       interval.seconds),
                                            max, na.rm = TRUE))
        aggregated <- merge.xts(success, error, elapsed.min, elapsed.max,
                                elapsed.median, elapsed.percentile99,
                                threads.median, concurrency.min,
                                concurrency.max)
    colnames(aggregated) <- str_c(prefix, ".", c("success.rate", "error.rate", 
      "elapsed.min", "elapsed.max", "elapsed.median", "elapsed.percentile99", 
      "threads.median", "concurrency.min", "concurrency.max"))
    aggregated
  }))
}

read_fping <- function(file.name) {
  as.xts(
    read.zoo(
      file.name,
      col.names = c("time", "seq", "rtt"),
      FUN = function(t) {as.POSIXct(t, origin = "1970-01-01 00:00:00")}))
}

aggregate_fping <- function(fping, interval.seconds) {
  ticks <- align.time(index(fping), interval.seconds)
  loss.raw <- diff(fping[, "seq"], na.pad = T) - 1
  loss <- as.xts(aggregate(loss.raw, ticks, sum, na.rm = TRUE)) /
    interval.seconds
  rtt.raw <- fping[, "rtt"] * 1000  # convert to microseconds
  rtt.min <- as.xts(aggregate(rtt.raw, ticks, min, na.rm = TRUE))
  rtt.max <- as.xts(aggregate(rtt.raw, ticks, max, na.rm = TRUE))
  rtt.median <- as.xts(aggregate(rtt.raw, ticks, median, na.rm = TRUE))
  rtt.percentile99 <- as.xts(aggregate(rtt.raw, ticks, quantile,
                                       probs = c(0.99), na.rm = TRUE))
  aggregated <- merge.xts(loss, rtt.min, rtt.max, rtt.median, rtt.percentile99)
  colnames(aggregated) <- c("loss", "fping.rtt.min", "fping.rtt.max",
                            "fping.rtt.median", "fping.rtt.percentile99")
  aggregated
}

draw_heatmap <- function(metric, bins = 500) {
  ggplot(fortify(metric, melt = T), aes(Index, Value)) +
    stat_bin2d(bins = bins) + 
    scale_fill_gradientn(colours = rainbow(7)) + 
    facet_grid(Series ~ .)
}

set_cores <- function(cores = detectCores()) {
  options(mc.cores = cores)
}

get_correlation_matrix <- function(metrics, complete = 0.1,
                                   method = "spearman", fill = 0.1) {
  counts <- sapply(metrics, function(x) {sum(!is.na(x))})
  n <- ncol(metrics)
  l <- nrow(metrics)
  d <- coredata(metrics)
  rl <- mclapply(combn(n, 2, simplify = FALSE), function(ij) {
    i <- ij[1]
    j <- ij[2]
    x2 <- d[, i]
    y2 <- d[, j]
    ok <- complete.cases(x2, y2)
    mc <- max(counts[i], counts[j])
    if (mc != 0 & sum(ok)/mc >= complete & sum(ok)/l >= fill) {
      x2 <- x2[ok]
      y2 <- y2[ok]
      cr <- cor(x2, y2, method = method)
      if (is.na(cr)) {
        cr <- 0
      }
    } else {
      cr <- 0
    }
    list(i, j, cr)
  })
  r <- matrix(0, nrow = n, ncol = n)
  for (ijc in rl) r[ijc[[1]], ijc[[2]]] <- ijc[[3]]
  r <- r + t(r) + diag(n)
  rownames(r) <- colnames(metrics)
  colnames(r) <- colnames(metrics)
  r
}

get_correlation_distance <- function(metrics, complete = 0.1,
                                     method = "spearman", fill = 0.1) {
  d <- as.dist(1 - abs(get_correlation_matrix(metrics, complete, method, fill)))
  d[d > 1] <- 1
  d[d < 0] <- 0
  d
}

filter_statsd <- function(metrics) {
  filter_colnames("sum(_50|_90|_99)$|mean(_50|_90|_99)?$|^stats_counts",
                  metrics, 
                  invert = TRUE)
}

filter_codahale_like <- function(metrics, counter.maxgap = 1) {
  metrics <- filter_colnames("\\.acceleration\\.[^\\.]+$|\\.(day|fifteen|five|one)$|\\.(mean|geometric_mean|harmonic_mean|kurtosis|skewness|standard_deviation|variance)$|\\.reductions_since_last_call$|\\.n$|\\.stddev$|MinuteRate$|\\.meanRate$|\\.publish\\.value$|MemtableColumnsCount\\.value$", 
    metrics, invert = TRUE)
  metrics <- metrics[,
                     which(sapply(metrics[], function(v) {any(!is.na(v))})),
                     drop = FALSE]
  columns <- colnames(metrics)
  counterNames <- grep("\\.number_of_gcs$|\\.words_reclaimed$|\\.io\\.input$|\\.io\\.output$|\\.total_reductions$|\\.count$|\\.vm\\.context_switches$|\\.runtime\\.total_run_time$|\\.jvm\\.gc.*(time|runs)$|\\.(CompletedTasks|TotalBlockedTasks|SpeculativeRetries|MemtableSwitchCount|BloomFilterFalsePositives|confirm|publish_in|publish_out|ack|deliver_get|deliver)\\.value$|_count$|\\.total\\.count$", 
                       columns, value = TRUE)
  counterNames <- grep("jvm\\.daemon_thread_count", counterNames,
                       invert = TRUE, value = TRUE)
  counters <- metrics[, counterNames, drop = FALSE]
  metrics <- exclude_columns(counters, metrics)
  counters[1, which(is.na(counters[1, ]))] <- 0
  counters.diff <- diff(na.locf(na.approx(counters, maxgap = counter.maxgap)), 
                        na.pad = TRUE)
  positive.jumps <- coredata(
    na.fill(lag.xts(diff(sign(counters.diff)) == 2, -1), FALSE))
  counters.diff[counters.diff < 0] <- 0
  counters.diff[positive.jumps] <- coredata(counters)[positive.jumps]
  metrics <- merge.xts(metrics, counters.diff)
  colnames(metrics) <- sub("org.apache.cassandra.metrics.", "",
                           colnames(metrics))
  metrics
}

filter_metrics <- function(metrics, change_threshold = 0.01) {
  cpu.columns <- grep("\\.cpu\\.[[:digit:]]+\\.cpu\\.(softirq|steal|system|user|wait|interrupt|idle)\\.value$", 
    colnames(metrics), value = TRUE)
  cpu.sums <- sapply(
    tapply(cpu.columns,
           sub("^(.*)\\.cpu\\.[[:digit:]]+\\.(.*)$", "\\1.\\2", cpu.columns), c),
    function(cl) {
      rowSums(metrics[, cl, drop = FALSE], na.rm = TRUE)
    })
  if (length(cpu.sums) > 0) {
    # TODO make it idempotent
    metrics <- cbind(metrics, cpu.sums)
  }
  metrics <-
    metrics[, !grepl("df_complex\\.used\\.value$|\\.disk\\.sd[a-z][0-9]\\.", 
                     colnames(metrics)),
            drop = FALSE]
  # TODO split into 2 functions here
  columns <- colnames(metrics)
  medians <- apply(metrics, 2, median, na.rm = TRUE)
  ranges <- apply(metrics, 2, range, na.rm = TRUE)
  metrics[,
          (!grepl("\\.cpu\\.[[:alpha:]]+\\.value$", columns) | ranges[2, ] > 5) &
            ranges[1, ] != ranges[2, ] &
            is.finite(ranges[1, ]) &
            is.finite(ranges[2,]) &
            (!grepl("load\\.(longterm|midterm|shortterm)$", columns) |
               ranges[2,] > 0.5) &
             abs((ranges[2, ] - ranges[1, ])/medians) > change_threshold  # TODO wrong for counters before diff
            & (!grepl("if_octets", columns) | ranges[2, ] > 1000) &
            (!grepl("if_packets", columns) | ranges[2, ] > 10), 
          drop = FALSE]
}

scale_range <- function(metrics) {
  ranges <- apply(metrics, 2, range, na.rm = TRUE)
  mins <- matrix(ranges[1, ], nrow = nrow(metrics), ncol = ncol(metrics), byrow = TRUE)
  maxs <- matrix(ranges[2, ], nrow = nrow(metrics), ncol = ncol(metrics), byrow = TRUE)
  (metrics - mins)/(maxs - mins)
}

get_relative_time <- function(metrics) {
  as.numeric(index(metrics) - min(index(metrics)))
}

get_abs_correlation <- function(x, metrics, subset = 1:nrow(metrics),
                                complete = 0.1, method = "spearman") {
  x <- coredata(x[subset])
  nx <- sum(!is.na(x))
  m <- coredata(metrics[subset, ])
  simplify2array(mclapply(1:ncol(m), function(k) {
    y <- m[, k]
    ok <- complete.cases(x, y)
    if ((sum(ok) / max(nx, sum(!is.na(y)))) > complete) {
      x2 <- x[ok]
      y2 <- y[ok]
      cr <- abs(cor(x2, y2, method = method))
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

## better for metrics with linear trends
get_diff_correlation <- function(x, metrics, ...) {
  get_abs_correlation(diff(x), diff(metrics), ...)
}

get_periodogram_distance <- function(x, metrics, subset = 1:nrow(metrics)) {
  simplify2array(mclapply(metrics, function(m) {
    diss.INT.PER(as.vector(x[subset]), as.vector(m[subset]), normalize = T)
  }))
}

exclude_columns <- function(what, from) {
  from[,
       setdiff(colnames(from), colnames(what)),
       drop=FALSE]
}

find_constant <- function(metrics, subset = 1:nrow(metrics)) {
  ranges <- sapply(metrics[subset, ], function(v) {
    range(v, na.rm = TRUE)
  })
  metrics[, ranges[1, ] == ranges[2, ], drop = FALSE]
}

filter_any_na <- function(metrics) {
  metrics[,
          which(sapply(metrics, function(v) {all(!is.na(v))})),
          drop = FALSE]
}

find_any_na <- function(metrics) {
  metrics[,
          which(sapply(metrics, function(v) {any(is.na(v))})),
          drop = FALSE]
}

find_na <- function(metrics, subset = 1:nrow(metrics)) {
  metrics[,
          which(sapply(metrics[subset, ], function(v) {all(is.na(v))})),
          drop = FALSE]
}

find_changed_sd <- function(metrics, a, b) {
  sd.a <- sapply(metrics[a, ], sd, na.rm = TRUE)
  sd.b <- sapply(metrics[b, ], sd, na.rm = TRUE)
  metrics[,
          order(sd.b / sd.a, decreasing = TRUE, na.last = TRUE),
          drop = FALSE]
}

find_changed_mean <- function(metrics, a, b) {
  mean.a <- sapply(metrics[a, ], mean, na.rm = TRUE)
  mean.b <- sapply(metrics[b, ], mean, na.rm = TRUE)
  metrics[,
          order(abs((mean.b - mean.a) / mean.a), decreasing = TRUE,
                na.last = TRUE), 
          drop = FALSE]
}

filter_colnames <- function(pattern, metrics, ...) {
  metrics[,
          grep(pattern, colnames(metrics), ...),
          drop = FALSE]
}

## TODO bring back dotted version
multiplot <- function(metrics, limit = 15, vline = NA) {
  data <- metrics
  if (length(colnames(data)) == 0) {
    colnames(data) <- 1:ncol(data)
  }
  data <- data[,
               which(sapply(data, function(v) {any(!is.na(v))})),
               drop = FALSE]
  r <- nrow(data)
  n <- ncol(data)
  i <- 1
  k <- min(n, limit)
  repeat {
    m <- data[, i:k, drop = FALSE]
    labels <- make.unique(colnames(m))
    df <- data.frame(index(m)[rep.int(1:r, ncol(m))],
                     factor(rep(1:ncol(m), each = r), levels = 1:ncol(m),
                            labels = labels),
                     as.vector(coredata(m)))
    names(df) <- c("Index", "Series", "Value")
    p <- ggplot(data = df) + geom_path(aes(x = Index, y = Value), na.rm = TRUE) + 
      xlab(NULL) + ylab(NULL) + facet_grid(Series ~ ., scales = "free_y") + 
      theme(strip.text.y = element_text(angle = 0))
    if (!is.na(vline)) {
      p <- p + geom_vline(xintercept = as.numeric(index(metrics)[vline]), colour = "red")
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
  multiplot(filter_colnames(pattern, metrics, ignore.case = TRUE), limit = limit)
}

sameplot <- function(metrics, ...) {
  autoplot(metrics, facet = NULL, ...)
}

# TODO pass function to compare
multiplot_sorted <- function(metrics, comparison, decreasing = TRUE, ...) {
  sort.order <- order(comparison, decreasing = decreasing)
  data <- metrics[,
                  sort.order,
                  drop = FALSE]
  colnames(data) <- paste("[", sort.order, "]",
                          comparison[sort.order],
                          names(metrics)[sort.order])
  multiplot(data, ...)
}

robust_histogram <- function(x, probs = c(0.01, 0.99), ...) {
  qplot(x = x, xlim = quantile(x, probs, na.rm = TRUE), ...)
}

find_breakpoints <- function(metrics, segment = 0.25) {
  bpl <- mclapply(metrics, function(m) {
    m <- na.omit(m)
    if ((segment < 1 & floor(segment * length(m)) <= 2) | segment > 1) {
      data.frame()
    } else {
      bp <- breakpoints(coredata(m) ~ get_relative_time(m), h = segment)$breakpoints
      if (is.na(bp)) {
        data.frame()
      } else {
        data.frame(name = names(m)[1], time = index(m)[bp])
      }
    }
  })
  result <- bind_rows(bpl)
  result[order(result$time), ]
}

get_distribution_change <- function(metric, window.seconds = 300,
                                    by.seconds = 60, p.value = 0.05, fill = 50) {
  index.range <- range(index(metric))
  half.window <- window.seconds %/% 2
  total.seconds <- as.integer(difftime(index.range[2], index.range[1], units = "s"))
  indices <- unique(align.time(index(metric), by.seconds))
  indices <- indices[indices > index.range[1] + half.window & indices < index.range[2] - half.window]
  values <- simplify2array(mclapply(indices, function(i) {
    x <- na.omit(as.vector(coredata(window(metric, start = i - half.window, end = i))))
    # TODO workaround for window() that includes both start and end,
    # expects metric to have 1s or larger intervals
    y <- na.omit(as.vector(coredata(window(metric, start = i + 1, end = i + half.window))))
    lx <- length(x)
    ly <- length(y)
    if (length(x) >= 1 & length(y) >= 1 &
        length(unique(x)) >= fill &
        length(unique(y)) >= fill) {
      ## TODO Cramer-von-Mises instead of Kolmogorov-Smirnov
      t <- ks.test(x, y, exact = FALSE)
      if (t$p.value < p.value) {
        t$statistic
      } else {
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


find_nonlinear <- function(metrics, subset = 1:nrow(metrics)) {
  diffs <- simplify2array(mclapply(as.ts(metrics[subset, ]), ndiffs))
  indices <- order(diffs, decreasing = TRUE)
  metrics[, indices[diffs[indices] > 1], drop = FALSE]
}

find_autocorrelated <- function(metrics, subset = 1:nrow(metrics), lag = 100, p.value = 0.05) {
  ac <- simplify2array(mclapply(metrics[subset, ], function(m) {
    bt <- Box.test(m, lag = lag, type = "Ljung-Box")
    c(bt$statistic, bt$p.value)
  }))
  indices <- na.exclude(order(ac[1, ], decreasing = TRUE))
  metrics[, na.exclude(indices[ac[2, indices] < p.value]), drop = FALSE]
}

get_autocorrelation <- function(metrics, subset = 1:nrow(metrics), lag) {
  simplify2array(mclapply(metrics[subset, ], function(m) {
    cor(m, lag(m, lag), use = "na.or.complete")
  }))
}

plot_medoids <- function(metrics, pamobject, limit = 50) {
  sorted <- order(pamobject$silinfo$clus.avg.widths * pamobject$clusinfo[, "size"], 
    decreasing = TRUE)
  sorted <- sorted[which(pamobject$clusinfo[sorted, "size"] > 1)]
  data <- metrics[,
                  pamobject$medoids[sorted],
                  drop = FALSE]
  colnames(data) <- paste("[", sorted, "]",
                          pamobject$medoids[sorted],
                          pamobject$clusinfo[sorted, "size"])
  multiplot(data, limit)
}

plot_cluster <- function(metrics, pamobject, id, limit = 50) {
  multiplot(
    metrics[,
            names(
              sort(
                pamobject$silinfo$widths[names(which(pamobject$clustering == id)),
                                         "sil_width"],
                decreasing = TRUE)),
            drop = FALSE],
    limit)
}

par_pam <- function(d, krange) {
  mclapply(krange, function(k) {
    pam(d, k, diss = TRUE)
  })
}

mc_period_apply <- function(metrics, INDEX, FUN, ...) {
  tree_merge_xts(mclapply(metrics, function(m) {
    period.apply(m, INDEX, FUN, ...)
  }))
}

mc_lm <- function(metrics) {
  rel.time <- get_relative_time(metrics)
  n <- names(metrics)
  lms <- mclapply(metrics, function(m) {
    fit <- lm(coredata(m) ~ rel.time, na.action = na.omit)
    list(residuals = residuals(fit), r.squared = summary.lm(fit)$r.squared)
  })
  m.r <- metrics
  coredata(m.r) <- sapply(lms, function(l) {
    l$residuals
  })
  list(residuals = m.r, r.squared = sapply(lms, function(l) {
    l$r.squared
  }))
}

mc_rq <- function(metrics) {
  rel.time <- get_relative_time(metrics)
  lms <- simplify2array(mclapply(metrics, function(m) {
    rq(coredata(m) ~ rel.time, na.action = na.omit)$residuals
  }))
  m.r <- xts(lms, order.by = index(metrics))
  names(m.r) <- names(metrics)
  m.r
}

decompose_median <- function(metrics, period) {
  half.window <- period %/% 2
  median.window <- half.window * 2 + 1
  l <- nrow(metrics)
  ld <- mclapply(metrics, function(m) {
    # TODO switch back to rollaply(median) to allow NAs in data
    trend <- runmed(coredata(m), median.window, endrule = "keep")
    trend[1:half.window] <- NA
    trend[(l - half.window):l] <- NA
    season <- coredata(m) - trend
    figure <- numeric(period)
    l <- length(m)
    index <- seq.int(1, l, by = period) - 1
    for (i in 1:period) figure[i] <- median(season[index + i], na.rm = TRUE)
    list(seasonal = rep(figure, l%/%period + 1)[seq_len(l)],
         trend = trend)
  })
  names(ld) <- NULL
  n <- ncol(metrics)
  idx <- index(metrics)
  nm <- names(metrics)
  uld <- unlist(ld, recursive = FALSE)
  t.m <- matrix(unlist(uld[names(uld) == "trend"], use.name = FALSE), ncol = n)
  trend <- xts(t.m, order.by = idx)
  names(trend) <- nm
  s.m <- matrix(unlist(uld[names(uld) == "seasonal"], use.names = FALSE), ncol = n)
  seasonal <- xts(s.m, order.by = idx)
  names(seasonal) <- nm
  list(trend = trend, seasonal = seasonal)
}

non_seasonal_proportion <- function(metrics, decomposed.metrics) {
  remainder <- abs(metrics - decomposed.metrics$trend - decomposed.metrics$seasonal)
  seasonal <- abs(decomposed.metrics$seasonal)
  sapply(colnames(metrics), function(m) {
    cc <- complete.cases(remainder[, m], seasonal[, m])
    sum(remainder[cc, m], na.rm = TRUE)/sum(seasonal[cc, m], na.rm = TRUE)
  })
}

maybe_deseason <- function(metrics, period, proportion = 0.3) {
  d <- decompose_median(metrics, period)
  nsp <- non_seasonal_proportion(metrics, d)
  seasonal <- metrics[, !is.na(nsp) & (nsp < proportion), drop = FALSE]
  other <- exclude_columns(seasonal, metrics)
  seasonal <- seasonal - d$seasonal[, names(seasonal)]
  names(seasonal) <- paste(names(seasonal), "deseason", sep = ".")
  merge.xts(seasonal, other)
}

find_outliers <- function(metrics, width, q.prob = 0.1, min.score = 5) {
  tree_merge_xts(mclapply(metrics, function(m) {
    rollapply(m, 2 * width + 1, fill = NA, align = "center", FUN = function(w) {
      # TODO check ranges as in filter metrics?
      left <- as.numeric(w[1:width])
      current <- as.numeric(w[width + 1])
      right <- as.numeric(w[(width + 1):(2 * width + 1)])  # includes current
      if (all(is.na(left))) {
        if (!is.na(current)) {
          4  # appeared
        } else {
          -4  # remained NA
        }
      } else if (is.na(current)) {
        if (!is.na(last(left)) & all(is.na(right))) {
          5  # disappeared
        } else {
          NA
        }
      } else { # current is not NA
        q <- quantile(left, probs = c(0, q.prob, 0.5, 1 - q.prob, 1),
                      na.rm = TRUE, type = 1)
        if (q[1] == q[5]) { # was constant
          if (current == q[1]) {
            -1  # remained the same
          } else {
            1  # constant changed
          }
        } else { # wasn't constant
          rr <- range(right, na.rm = TRUE)
          if (is.finite(rr[1]) & (rr[1] == rr[2]) & (is.na(last(left)) | (last(left) != rr[1]))) {
            6  # became constant
          } else {
            dq <- q[4] - q[2]
            r <- q[5] - q[1]
            lc <- current - q[3]
            if (dq == 0) { # majority ~= median
              if (abs(lc/r) > 1) {
                3  # outside min-max range
              } else {
                -3  # inside min-max range
              }
            } else {
              if ((abs(lc/dq) > min.score) & (abs(lc/r) > 1)) {
                2  # outside iqr and min-max range
              } else {
                -2  # inside iqr or min-max range
              }
            }
          }
        }
      }
    })
  }))
}

sum_xts_rows <- function(metrics) {
  xts(rowSums(metrics, na.rm = T), index(metrics))
}

find_sparse <- function(metrics, fill = 0.1) {
  l <- nrow(metrics)
  metrics[,
          which(sapply(metrics, function(v) {(sum(is.na(v))/l) > fill})),
          drop = FALSE]
}

svd_prepare <- function(metrics) {
  m <- metrics[2:nrow(metrics), ]  # first row is NA for counters
  m <- exclude_columns(find_sparse(m), m)
  m <- exclude_columns(find_constant(m), m)
  na.approx(m)
}

# then use multiplot_sorted(metrics, abs(udv$v[,component]))
svd_u_xts <- function(udv, metrics) {
  xts(udv$u, order.by = index(metrics))
}

get_top_loadings <- function(loadings, n = 3) {
  as.vector(apply(abs(loadings), 2, order, decreasing = T)[1:n, ])
}

get_non_zero_columns <- function(metrics) {
  colnames(metrics)[which(colSums(metrics > 0, na.rm = TRUE) > 0)]
}

mc_xts_apply <- function(metrics, FUN, ...) {
  tree_merge_xts(mclapply(metrics, function(m) {
    FUN(m, ...)
  }))
}

# then find outliers to get mean shifts
diff_median <- function(metrics, window) {
  mc_xts_apply(metrics, function(m) {
    diff(rollapply(m, window, fill = NA, align = "center", FUN = median, na.rm = T), 
      na.pad = TRUE)
  })
}

widen <- function(x, width) {
  lags <- tree_merge_xts(lapply(-width:width, function(n) {
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

find_periods <- function(metrics, significance = 0.99, ...) {
  nfp <- mclapply(
    metrics,
    function(m) {
      spec <- spec.mtm(as.ts(m), plot = FALSE, Ftest = TRUE,
                       returnZeroFreq = FALSE, ...)
      f.sig <- spec$mtm$Ftest > qf(significance, 2, 2 * spec$mtm$k - 2)
      if (any(f.sig, na.rm = TRUE)) {
        data.frame(name = names(m)[1],
                   period = 1/spec$freq[f.sig],
                   Ftest = spec$mtm$Ftest[f.sig])
      } else {
        data.frame()
      }
    })
  result <- rbind.fill(nfp)
  result$name <- as.character(result$name)
  result[order(result$Ftest), ]
}

remove_variable <- function(metrics, variable) {
  lms <- simplify2array(mclapply(metrics, function(m) {
    rq(coredata(m) ~ variable, na.action = na.omit)$residuals
  }))
  m.r <- xts(lms, order.by = index(metrics))
  names(m.r) <- names(metrics)
  m.r
}

# works for cmdscale and tsne TODO allow zooming
explore_2d <- function(embedding, metrics) {
  embedding_df <- data.frame(x = embedding[, 1], y = embedding[, 2])
  rownames(embedding_df) <- colnames(metrics)
  app <- shinyApp(ui = fluidPage(helpText("select points to draw series"), plotOutput("embedding_plot", 
    click = "embedding_click"), dygraphOutput("series")), server = function(input, 
    output) {
    output$embedding_plot <- renderPlot({
      ggplot(embedding_df, aes(x, y)) + geom_point()
    })
    output$series <- renderDygraph({
      n <- rownames(nearPoints(embedding_df, input$embedding_click))[1]
      dygraph(metrics[, n, drop = FALSE], main = n)
    })
  })
  runApp(app)
}

drop_zero_dist <- function(d) {
  m <- as.matrix(d)
  z <- which(m == 0, arr.ind = TRUE)
  di <- z[z[, 1] > z[, 2], 1]
  mu <- m[-di, -di]
  as.dist(mu)
} 

shinyplot <- function(metrics, limit = 100) {
  data <- metrics
  if (length(colnames(data)) == 0) {
    colnames(data) <- 1:ncol(data)
  }
  data <- data[,
               which(sapply(data, function(v) {any(!is.na(v))})),
               drop = FALSE]
  data <- data[, 1:min(limit, ncol(data))]
  app <- shinyApp(
    ui = fluidPage(
      lapply(
        1:ncol(data),
        function(n) {
          fluidRow(
            column(
              dygraphOutput(paste("graph_series_", n, sep = ""),
                            height = "100px"),
              width = 6),
            column( # TODO center text vertically
              textOutput(paste("text_series_", n, sep = "")),
              width = 6))
        })
      ),
    server = function(input, output) {
      lapply(
        1:ncol(data),
        function(n) {
          single <- data[, n, drop = FALSE]
          output[[paste("graph_series_", n, sep = "")]] <- renderDygraph({
            dygraph(single, group = "series") %>%
              dyLegend(show = "never") %>%
              dyOptions(drawXAxis = (n %% 9 == 0) | (n == ncol(data)))
          })
          output[[paste("text_series_", n, sep = "")]] <- renderText({
            colnames(single)
          })
          
        })
    })
  runApp(app)
}
