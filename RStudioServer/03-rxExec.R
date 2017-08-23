############################################################################
# Run open source R functions in a distributed context on partitioned data #
############################################################################

##
## Compute average departure delay by day of the week
##

#
# Start Spark session
#
cc <- rxSparkConnect(reset = TRUE)

#
# Define function to compute average delay by grouping key in a single data set
#
AverageDelay <- function(keys, data) {
  df <- rxImport(data)
  mean(df$ArrDelay, na.rm = TRUE)
}

#
# Define column info for the airline departure data set
#
colInfo <-
  list(
    ArrDelay = list(type = "numeric"),
    CRSDepTime = list(type = "numeric"),
    DayOfWeek = list(type = "string")
  )

#
# Create a text data source with the airline data
#
textData <-
  RxTextData(
    file = "/example/data/MRSSampleData/AirlineDemoSmall.csv",
    firstRowIsColNames = TRUE,
    colInfo = colInfo,
    fileSystem = RxHdfsFileSystem()
  )

#
# Group textData by day of week and get average delay on each day
#
meanDelaysList <- rxExecBy(inData = textData, keys = c("DayOfWeek"), func = AverageDelay)

#
# transform objs to a data frame
#
do.call(rbind, lapply(meanDelaysList, unlist))


##
## Build linear models for each day of the week to predict arrival delay based on departure time
##

#
# Define function to run linear regression on data partition 
#
delayFunc <- function(keys, data) { 
  df <- rxImport(inData = data) 
  rxLinMod(ArrDelay ~ CRSDepTime, data = df) 
} 

#
# Create a text data source with the airline data
#
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

#
# Group airlineData by day of week and get average delay on each day
#
modelList <- rxExecBy(inData=airlineData, keys=c("DayOfWeek"), func=delayFunc)  

#
# Print summary of first model
#
modelList[[1]]$key
modelList[[1]]$result

#
# Close the connection to Spark
#
rxSparkDisconnect(cc)