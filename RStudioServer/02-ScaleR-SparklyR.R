# Install sparklyr
# install.packages("sparklyr")


# Load required libraries
library(RevoScaleR)
library(sparklyr)
library(dplyr)


# Connect to Spark using rxSparkConnect, specifying 'interop = "sparklyr"'
# this will create a sparklyr connection to spark, and allow you to use
# dplyr for data manipulation. Using rxSparkConnect in this way will use
# default values and rxOptions for creating a Spark connection, please
# see "?rxSparkConnect" for how to define parameters specific to your setup
cc <- rxSparkConnect(reset = TRUE, interop = "sparklyr")


# The returned Spark connection (sc) provides a remote dplyr data source 
# to the Spark cluster using SparlyR within rxSparkConnect.
sc <- rxGetSparklyrConnection(cc)


# Next, load mtcars in to Spark using a dplyr pipeline
mtcars_tbl <- copy_to(sc, mtcars)


# Now, partition the data into Training and Test partitions using dplyr
partitions <- mtcars_tbl %>%
  filter(hp >= 100) %>%
  mutate(cyl8 = cyl == 8) %>%
  sdf_partition(training = 0.5, test = 0.5, seed = 1099)


# Register the partitions as DataFrames using sparklyr
sdf_register(partitions$training, "cars_training")
sdf_register(partitions$test, "cars_test")



# Create a RxHiveData Object for each
cars_training_hive <- RxHiveData(table = "cars_training", 
                                 colInfo = list(cyl = list(type = "factor")))
cars_test_hive <- RxHiveData(table = "cars_test", 
                             colInfo = list(cyl = list(type = "factor")))

# Use the Training set to train a model with rxLinMod()
lmModel <- rxLinMod(mpg ~ wt + cyl, cars_training_hive)


# Take a summary of the trained model (this step is optional)
summary(lmModel)


# Currently, for rxPredict(), only XDF files are supported as an output
# The following command will create the directory "/user/RevoShare/MTCarsPredRes" 
# to hold the composite XDF.
pred_res_xdf <- RxXdfData("/user/RevoShare/MTCarsPredRes", fileSystem = RxHdfsFileSystem())


# Run rxPredict, specifying the outData as our RxXdfData() object, write
# the model variables into the results object so we can analyze out accuracy
# after the prediction completes
pred_res_xdf <- rxPredict(lmModel, data = cars_test_hive, outData = pred_res_xdf, 
                          overwrite = TRUE, writeModelVars = TRUE)


# Now, import the results from HDFS into a DataFrame so we can see our error
pred_res_df <- rxImport(pred_res_xdf)


# Calculate the Root Mean Squared error of our prediction
sqrt(mean((pred_res_df$mpg - pred_res_df$mpg_Pred)^2, na.rm = TRUE))


# When you are finished, close the connection to Spark
rxSparkDisconnect(cc)


# # # # # 
# It is assumed at this point that you have installed sparklyr
# Proceed to load the required libraries
library(RevoScaleR)
library(sparklyr)
library(dplyr)


# Connect to Spark using rxSparkConnect
cc <- rxSparkConnect(reset = TRUE, interop = "sparklyr")


# The returned Spark connection (sc) provides a remote dplyr data source 
# to the Spark cluster using SparlyR within rxSparkConnect.
sc <- rxGetSparklyrConnection(cc)


# Bind RxHdfsFileSystem to a variable, this will reduce code clutter
hdfsFS <- RxHdfsFileSystem()


# # # #
# There are many data sources which we can use in MRS, in the this section
# we will go through 4 different file based data sources and how to import 
# data for use in Spark. If you wish to use any of these data sources, simply
# comment out line number 40
#
# One data source is XDF files stored in HDFS. Here we create an Data Object from
# a composite XDF from HDFS, this can then be held in memory as a DataFrame, or 
# loaded into a Hive Table.
AirlineDemoSmall <- RxXdfData(file="/example/data/MRSSampleData/AirlineDemoSmallComposite", fileSystem = hdfsFS)
# Another option is CSV files stored in HDFS. To create a CSV Data Object from HDFS 
# we would use RxTextData(). This can also be used for othere plain text type formats
AirlineDemoSmall <- RxTextData("/example/data/MRSSampleData/AirlineDemoSmall.csv", fileSystem = hdfsFS)
# A third option is Parquet data using RxParquetData()
AirlineDemoSmall <- RxParquetData("/example/data/MRSSampleData/AirlineDemoSmallParquet", fileSystem = hdfsFS)
# Lastly, ORC  Data using RxOrcData()
AirlineDemoSmall <- RxOrcData("/example/data/MRSSampleData/AirlineDemoSmallOrc", fileSystem = hdfsFS)
# # # #


# Continuing with our example, first, prepare your data for loading, for this, 
# I'll proceed with XDF data, but any of the previously specified data sources 
# are valid.
AirlineDemoSmall <- RxXdfData(file="/example/data/MRSSampleData/AirlineDemoSmallComposite", fileSystem = hdfsFS)


# Regardless of which data source used, to work with it using dplyr, we need to 
# write it to a Hive table, so, Next, create a Hive Data Object using RxHiveData()
AirlineDemoSmallHive <- RxHiveData(table="AirlineDemoSmall")


# Use rxDataStep to load the data into the table
# this depends on data
rxDataStep(inData = AirlineDemoSmall, outFile = AirlineDemoSmallHive,
           overwrite = TRUE) # takes about 90 seconds on 1 node cluster


###
#
# If you wanted the data as a data frame in Spark, you would use
# rxDataStep() like so:
#
# AirlineDemoSmalldf <- rxDataStep(inData = AirlineDemoSmall, 
#            overwrite = TRUE) 
#
# But data must be in a Hive Table for use with dplyr as an In-Spark 
# object
###


# To see that the table was created list all Hive tables
src_tbls(sc)


# Next, define a dplyr data source referencing the Hive table
# This caches the data in Spark
tbl_cache(sc, "airlinedemosmall")
flights_tbl <- tbl(sc, "airlinedemosmall")


# Print out a few rows of the table
flights_tbl


# Filter the data to remove missing observations
model_data <- flights_tbl %>%
  filter(!is.na(ArrDelay)) %>%
  select(ArrDelay, CRSDepTime, DayOfWeek)


# Now, partition data into training and test sets using dplyr
model_partition <- model_data %>% 
  sdf_partition(train = 0.8, test = 0.2, seed = 6666)


# Fit a linear model using Spark's ml_linear_regression to attempt
# to predict CRSDepTime by Day of Week and Delay
ml1 <- model_partition$train %>%
  ml_linear_regression(CRSDepTime ~ DayOfWeek + ArrDelay)


# Run a summary on the model to see the estimated CRSDepTime per Day of Week
summary(ml1)


# When you are finished, close the connection to Spark
rxSparkDisconnect(cc)