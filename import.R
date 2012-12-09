files <- list.files()

readfile <- function(name) {
  read.table(name, na.strings="None", colClasses=c("numeric", "numeric"), col.names=c("time", name))
}
