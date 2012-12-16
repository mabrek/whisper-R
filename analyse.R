library(forecast)

correlate <- function(x) {
  correlated <- cor(x, use="pairwise.complete.obs")
  correlated[is.na(correlated)] <- 0
}

distance <- function(correlated) {
  as.dist(1-abs(correlated))
}

# the code below is based on ChangeAnomalyDetection package
arimaScore <- function (x, term = 30, ...) 
{
  sapply(1:(length(x) - term - 1), function(i) {
    train <- x[i:(i + term)]
    target <- x[(i + term + 1)]
    fit <- auto.arima(train, ...)
    pred <- forecast(fit, h = 1)$mean[1]
    m <- mean(fit$residuals)
    s <- sd(fit$residuals)
    -log(dnorm(pred - target, m, s))
  })
}

linearScore <- function (x, term = 30, ...) {
  sapply(1:(length(x) - term), function(i) {
    train <- x[i:(i + term)]
    fit <- lm(y ~ x, data.frame(x=1:term, y=train))
    # TODO measure fit
  })
}   
