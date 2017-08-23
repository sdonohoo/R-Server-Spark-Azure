ts1.sim <- arima.sim(list(order = c(1,0,0), ar = 0.7), n = 200)
ts2.sim <- arima.sim(list(order = c(1,0,0), ar = 0.7), n = 200)
ts.plot(ts1.sim)
ts.plot(ts2.sim)
model <- arima(ts.sim, order = c(1, 0, 0))
summary(model)


timeSeriesModel <- function(i)
{
  setMKLthreads(1)
  txt <- RxTextData(paste0("timeseriesfolder/sku", i, ".csv"), delimiter = ",", fileSystem = fileSystemToUse)
  x <- rxDataStep(txt)$x
  model <- arima(x, order = c(1, 0, 0))
  return(model)
}

cc <- rxSparkConnect(reset = TRUE) 

fileSystemToUse <- RxHdfsFileSystem()
results <- rxExec(timeSeriesModel, elemArgs = 1:1000, execObjects = c("fileSystemToUse"))

rxSparkDisconnect(cc)


cc <- rxSparkConnect(reset = TRUE) 

delayFunc <- function(keys, data) { 
  df <- rxImport(inData = data) 
  rxLinMod(ArrDelay ~ CRSDepTime, data = df) 
} 

airlineData <- 
  RxTextData( 
    "/example/data/MRSSampleData/AirlineDemoSmall.csv", 
    firstRowIsColNames = TRUE,
    colInfo = list( 
      ArrDelay = list(type = "numeric"), 
      DayOfWeek = list(type = "factor") 
    ), 
    fileSystem = RxHdfsFileSystem() 
  ) 

returnObjs <- rxExecBy(inData=airlineData, keys=c("DayOfWeek"), func=delayFunc)  

returnObjs[[1]]$key
returnObjs[[1]]$result

# transform objs to a data frame
do.call(rbind, lapply(returnObjs, unlist))

rxSparkDisconnect(cc)

##############################################################################
# run analytics with RxSpark compute context
##############################################################################
# start Spark app
sparkCC <- rxSparkConnect()

# define function to compute average delay
".AverageDelay" <- function(keys, data) {
  df <- rxDataStep(data)
  mean(df$ArrDelay, na.rm = TRUE)
}

# define colInfo
colInfo <-
  list(
    ArrDelay = list(type = "numeric"),
    CRSDepTime = list(type = "numeric"),
    DayOfWeek = list(type = "string")
  )

# create text data source with airline data
textData <-
  RxTextData(
    file = "/example/data/MRSSampleData/AirlineDemoSmall.csv",
    firstRowIsColNames = TRUE,
    colInfo = colInfo,
    fileSystem = RxHdfsFileSystem()
  )

# group textData by day of week and get average delay on each day
objs <- rxExecBy(textData, keys = c("DayOfWeek"), func = .AverageDelay)

# transform objs to a data frame
do.call(rbind, lapply(objs, unlist))

# stop Spark app
rxSparkDisconnect(sparkCC)