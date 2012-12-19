library(foreach)
library(doParallel)
library(plyr)

read.file <- function(file.name) {
  data <- na.omit(
    read.table(
      file.name,
      na.strings="None",
      colClasses=c("integer", "numeric"),
      col.names=c("time", basename(file.name))))
  if (nrow(data) == 0)
    NA
  else if (min(data[[2]]) == max(data[[2]]))
    NA
  else
    data
}

merge.metrics <- function(x,y) {
  if (all(is.na(x)))
    y
  else if(all(is.na(y)))
    x
  else
    join(x, y, by="time", type="full", match="first")
}

load.metrics <- function(path=".") {
  # TODO combine in parallel
  metrics <- foreach(f=list.files(path, full.names=TRUE), .combine=merge.metrics, .packages="plyr") %dopar% {
    read.file(f)
  }
  metrics$relTime <- metrics$time - min(metrics$time)
  metrics
}

set.cores <- function(cores = detectCores()) {
  registerDoParallel(cores)
}
