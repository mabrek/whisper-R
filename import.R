files <- list.files()

readfile <- function(name) {
  data <- na.exclude(read.table(name, na.strings="None", colClasses=c("integer", "numeric"), col.names=c("time", name)))
  if(nrow(data) == 0) {
     NA
   } else {
     if (min(data[[2]]) == max(data[[2]])) NA else data
   }
}

# use parallel versions for reading and merging
allMetrics <- lapply(files, readfile)

existingMetrics <- allMetrics[!is.na(allMetrics)]

# http://www.r-bloggers.com/merging-multiple-data-files-into-one-data-frame/
mergedMetrics <- Reduce(function(x, y) {merge(x,y, by="time", all=TRUE)}, existingMetrics)
