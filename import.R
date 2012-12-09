files <- list.files()

readfile <- function(name) {
  read.table(name, na.strings="None", colClasses=c("numeric", "numeric"), col.names=c("time", name))
}

allMetrics <- lapply(files, readfile)

# http://www.r-bloggers.com/merging-multiple-data-files-into-one-data-frame/

mergedMetrics <- Reduce(function(x, y) {merge(x,y)}, allMetrics)
