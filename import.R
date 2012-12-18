library(parallel)

files <- list.files()

readfile <- function(name) {
  data <- na.exclude(read.table(name, na.strings="None", colClasses=c("integer", "numeric"), col.names=c("time", name)))
  if(nrow(data) == 0) {
     NA
   } else {
     # TODO exclude noise-like and mostly constant
     if (min(data[[2]]) == max(data[[2]])) NA else data
   }
}

allMetrics <- mclapply(files, readfile, mc.allow.recursive = FALSE)

existingMetrics <- allMetrics[!is.na(allMetrics)]

# http://www.r-bloggers.com/merging-multiple-data-files-into-one-data-frame/
# TODO consider plyr::join or read and merge in parallel (package foreach?)
mergedMetrics <- Reduce(function(x, y) {merge(x,y, by="time", all=TRUE)}, existingMetrics)
