############################################################################################
# Logistic regression on 2012 airline delay dataset showing ScaleR operation in both local #
# and Spark compute contexts.                                                              #
#                                                                                          #
# Medium airline data set has 6096762 rows and 32 columns.                                 #
############################################################################################

#####################################
# Get sample airline departure data #
#####################################

# Create a local folder for storing data temporarily.
localDir <- "/tmp/AirOnTimeCSV2012"
dir.create(localDir)

# Download data to the tmp folder.
remoteDir <- "https://packages.revolutionanalytics.com/datasets/AirOnTimeCSV2012"
download.file(file.path(remoteDir, "airOT201201.csv"), file.path(localDir, "airOT201201.csv"))
download.file(file.path(remoteDir, "airOT201202.csv"), file.path(localDir, "airOT201202.csv"))
download.file(file.path(remoteDir, "airOT201203.csv"), file.path(localDir, "airOT201203.csv"))
download.file(file.path(remoteDir, "airOT201204.csv"), file.path(localDir, "airOT201204.csv"))
download.file(file.path(remoteDir, "airOT201205.csv"), file.path(localDir, "airOT201205.csv"))
download.file(file.path(remoteDir, "airOT201206.csv"), file.path(localDir, "airOT201206.csv"))
download.file(file.path(remoteDir, "airOT201207.csv"), file.path(localDir, "airOT201207.csv"))
download.file(file.path(remoteDir, "airOT201208.csv"), file.path(localDir, "airOT201208.csv"))
download.file(file.path(remoteDir, "airOT201209.csv"), file.path(localDir, "airOT201209.csv"))
download.file(file.path(remoteDir, "airOT201210.csv"), file.path(localDir, "airOT201210.csv"))
download.file(file.path(remoteDir, "airOT201211.csv"), file.path(localDir, "airOT201211.csv"))
download.file(file.path(remoteDir, "airOT201212.csv"), file.path(localDir, "airOT201212.csv"))

###########################################################################
# Describe the data the script will operate on from the airline data set. #
###########################################################################

#
# Create columnInfo object for the subset of columns we want to use.
#
airlineColInfo <- list(
  DAY_OF_WEEK = list(type = "factor"),
  ORIGIN = list(type = "factor"),
  DEST = list(type = "factor"),
  DEP_TIME = list(type = "integer"),
  ARR_DEL15 = list(type = "logical"))

# Get all the column names.
varNames <- names(airlineColInfo)

################################
# Run rxLogit on local data set
################################

#
# Define a text data source in local system for the airline data
#
airOnTimeDataLocal <- RxTextData(localDir,
                                 colInfo = airlineColInfo,
                                 varsToKeep = varNames)

#
# Define the formula to use for the logistic regression (rxLogit)
# The model will predict whether a flight will be delayed by at least 15 minutes based
# on Origin airport and day of the week.
#
formula = "ARR_DEL15 ~ ORIGIN + DAY_OF_WEEK"

#
# Set the compute context to local.
#
rxSetComputeContext("local")

#
# Run a logistic regression on the local airline .csv data in a local compute
# context (edge node only).
# 
# Operation takes ~5:50 to complete on a D4v2 edge node.
# 
system.time(
  modelLocal <- rxLogit(formula, data = airOnTimeDataLocal)
)

#
# Display a summary of the model
#
summary(modelLocal)


#############################################################
# Perform the same logistic regression on data in HDFS in a #
# Spark compute context.                                    #
#############################################################

#
# Set the HDFS (WASB) location of example data.
#
bigDataDirRoot <- "/example/data"

#
# Set directory in bigDataDirRoot to load the data.
#
inputDir <- file.path(bigDataDirRoot,"AirOnTimeCSV2012")

#
# Create the directory.
#
rxHadoopMakeDir(inputDir)

#
# Copy the data from source to input.
#
rxHadoopCopyFromLocal(localDir, bigDataDirRoot)

#
# Define reference to HDFS filesystem in the cluster
#
hdfsFS <- RxHdfsFileSystem()

#
# Define the text data source in HDFS.
#
airOnTimeData <- RxTextData(inputDir,
                            colInfo = airlineColInfo,
                            varsToKeep = varNames,
                            fileSystem = hdfsFS)

#
# Start the Spark session.
#
cc <- rxSparkConnect(reset=TRUE)

#
# Run the logistic regression on the  airline .csv data in HDFS in a Spark compute
# context.
# 
# Operation takes ~1:08 to complete on 3 worker nodes.
# 
system.time(
  modelSpark <- rxLogit(formula, data = airOnTimeData)
)

#
# Display a summary.
#
summary(modelSpark)

#
# Close the connection to Spark
#
rxSparkDisconnect(cc)

