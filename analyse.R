library(forecast)

correlate <- function(x) {
  correlated <- cor(x, use="pairwise.complete.obs")
  correlated[is.na(correlated)] <- 0
}

distance <- function(correlated) {
  as.dist(1-abs(correlated))
}

linearScoreVector <- function (x, term = 30, ...) {
  score <- sapply(1:(length(x) - term), function(i) {
    train <- x[i:(i + term - 1)]
    fit <- lm(b ~ a, data.frame(a=1:term, b=train))
    1 - summary(fit)$adj.r.squared
  })
  c(rep(0, round(term/2)), score, rep(0, term - round(term/2)))
}   
