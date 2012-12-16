correlate <- function(x) {
  correlated <- cor(x, use="pairwise.complete.obs")
  correlated[is.na(correlated)] <- 0
}

distance <- function(correlated) {
  as.dist(1-abs(correlated))
}
