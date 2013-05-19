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

read.file <- function(file.name) {
  as.xts(read.zoo(
    file.name,
    na.strings="None",
    colClasses=c("integer", "numeric"),
    col.names=c("time", basename(file.name)),
    FUN=function(t) {as.POSIXct(t, origin="1970-01-01 00:00.00", tz="UTC")},
    drop=FALSE))
}

load.metrics <- function(path=".") {
  merge.files(list.files(path, full.names=TRUE))
}

merge.files <- function(files) {
  k <- length(files)
  if (k == 1) {
    read.file(files[1])
  } else if (k > 1) {
    merge.xts(merge.files(files[1 : (k %/% 2)]),
              merge.files(files[(k %/% 2 + 1) : k]))
  }
}

set.cores <- function(cores = detectCores()) {
  options(mc.cores = cores)
}

get.correlation.distance <- function(metrics, complete=0.1, method="spearman") {
  counts <- sapply(metrics, function(x) {sum(!is.na(x))})
  n <- ncol(metrics)
  d <- coredata(metrics)
  rl <- mclapply(combn(n, 2, simplify=FALSE), function(ij) {
    i <- ij[1]
    j <- ij[2]
    x2 <- d[,i]
    y2 <- d[,j]
    ok <- complete.cases(x2, y2)
    if ((sum(ok) / max(counts[i], counts[j])) > complete) {
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
  as.dist(1-abs(r))
}

filter.metrics <- function(metrics, change.threshold=0.05) {
  cpu.columns <- grep("\\.cpu\\.[[:digit:]]+\\.cpu\\.(softirq|steal|system|user|wait)\\.value$",
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
                     !grepl("sum(_50|_90|_99)$|mean(_50|_90|_99)?$|^stats_counts|df_complex\\.used\\.value$|\\.cpu\\.[[:digit:]]+\\.cpu\\.|\\.disk\\.sd[a-z][0-9]\\.", colnames(metrics)),
                     drop=FALSE]
  columns <- colnames(metrics)
  means <- apply(metrics, 2, mean, na.rm=TRUE)
  sds <- apply(metrics, 2, sd, na.rm=TRUE)
  ranges <- apply(metrics, 2, range, na.rm=TRUE)
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

find.correlated <- function(x, metrics, subset=1:nrow(metrics), complete=0.1, method="spearman") {
  x <- coredata(x[subset])
  nx <- sum(!is.na(x))
  m <- coredata(metrics[subset,])
  correlation <- simplify2array(mclapply(1:ncol(m), function(k) {
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
  metrics[,
          order(correlation, decreasing=TRUE),
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

filter.colnames <- function(pattern, metrics, invert=FALSE) {
  metrics[,
          grep(pattern, colnames(metrics), invert=invert),
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
          seq.int(r) %/% max(10, r %/% 500),
          FUN=function(x) {mean(x, na.rm=T)})
    })
    df <- data.frame(index(m)[rep.int(1:r, ncol(m))],
                     factor(rep(1:ncol(m), each = r), levels = 1:ncol(m), labels = colnames(m)),
                     as.vector(coredata(m)),
                     as.vector(coredata(ms)))
    names(df) <- c("Index", "Series", "Value", "Smooth")
    p <- ggplot(data = df) + geom_point(aes(x = Index, y = Value), na.rm=TRUE, shape=".") + geom_path(aes(x = Index, y = Smooth), na.rm=TRUE, color="blue") + xlab("") + ylab("") + facet_grid(Series ~ ., scales = "free_y") + theme(strip.text.y = element_text(angle=0), axis.text.y = element_blank(), axis.ticks.y = element_blank())
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

find.distribution.change <- function(metrics, startup=50) {
  cpa <- simplify2array(mclapply(seq.int(ncol(metrics)), function(n) {
    m <- na.omit(coredata(metrics[,n]))
    a <- NA
    for (arl in  c(seq(2000, 1000, -1000), seq(900, 500, -100), 370)) {
      if (detectChangePoint(m, cpmType="Kolmogorov-Smirnov", arl, startup)$changePoint > 0) {
        cat (n, " ", arl, "\n")
        a <- arl
        break;
      }
    }
    a
  }))
  cat (cpa, "\n")
  indices <- order(cpa, na.last=TRUE)
  metrics[,
          indices[!is.na(cpa[indices])],
          drop=FALSE]
}

find.nonlinear <- function(metrics, subset=1:nrow(metrics)) {
  diffs <- simplify2array(mclapply(metrics[subset,], ndiffs))
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
