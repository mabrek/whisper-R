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
    1 - summary(lm(b ~ a, df, ...))$adj.r.squared
  })
  c(rep(0, round(term/2)), score, rep(0, term - round(term/2)))
}   

linearScore <- function (df, axis = "time", ...) {
  columns = colnames(df)[colnames(df) != axis]
  scored = lapply(df[columns], function(x) {
    linearScoreVector(df[[axis]], x, ...)
  })
  scored[[axis]] = df[[axis]]
  as.data.frame(scored)
}
