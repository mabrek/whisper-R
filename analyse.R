library(parallel)

correlate <- function(x) {
  correlated <- cor(x, use="pairwise.complete.obs")
  correlated[is.na(correlated)] <- 0
}

distance <- function(correlated) {
  as.dist(1-abs(correlated))
}

linearScoreVector <- function (x, y, term = 30, ...) {
  missingY = missing(y)
  score <- sapply(1:(length(x) - term), function(i) {
    if(missingY) {
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

linearScore <- function (df, axis = "relTime", ...) {
  columns = colnames(df)[colnames(df) != axis]
  lsv = function(x) {
    linearScoreVector(df[[axis]], x, ...)
  }
  scored = mclapply(df[columns], lsv, mc.allow.recursive = FALSE)
  scored[[axis]] = df[[axis]]
  as.data.frame(scored)
}
