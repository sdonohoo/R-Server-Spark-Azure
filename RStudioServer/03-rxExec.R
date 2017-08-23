##################################################################################################
# Use rxExec and rxExecBy to run open source R code in a distributed context for highly-parallel #
# simulation or on partitioned data                                                              #
##################################################################################################

##
## Use rxExec to simulate 10000 games of craps across Spark cluster.
## If you roll a 7 or 11 on your initial roll, you win. If you roll 2, 3, or 12, you lose.
## Roll a 4, 5, 6, 8, 9, or 10, NS that number becomes your point and you continue rolling
## until you either roll your point again (in which case you win) or roll a 7, in which case you lose.
##

#
# Start Spark session
#
cc <- rxSparkConnect(reset = TRUE) 

playDice <- function()
{
  result <- NULL
  point <- NULL
  count <- 1
  while (is.null(result))
  {
    roll <- sum(sample(6, 2, replace=TRUE))
    
    if (is.null(point))
    {
      point <- roll
    }
    if (count == 1 && (roll == 7 || roll == 11))
    {
      result <- "Win"
    }
    else if (count == 1 && (roll == 2 || roll == 3 || roll == 12))
    {
      result <- "Loss"
    }
    else if (count > 1 && roll == 7 )
    {
      result <- "Loss"
    }
    else if (count > 1 && point == roll)
    {
      result <- "Win"
    }
    else
    {
      count <- count + 1
    }
  }
  result
}

z <- rxExec(playDice, timesToRun=10000, taskChunkSize=3000)
table(unlist(z))


##
## Compute average departure delay by day of the week
##


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