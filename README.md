
This repo contains demo R code the <a href="https://www.meetup.com/Data-AI-Microsoft/">Microsoft Data and AI Meetup</a>, "Data science using distributed R -  Microsoft R server + Spark + Azure" presentation.

To run the demo code, you will need to provision an HDInsight 3.6 "R Server" type cluster with R Server 9.1 and Spark 2.1.

Go to https://portal.azure.com and create the HDInsight cluster. Default VM size and number of worker nodes are sufficient.

After the cluster is provisioned (about 20 minutes), ssh to the edge node in the cluster using the ssh user and password you specified when provisioning the cluster:

```
ssh USERNAME@CLUSTERNAME-ed-ssh.azurehdinsight.net
```

Run the following commands in the shell to download the RStudio Server project and copy R Server sample data sets to HDFS:
```
sudo apt install subversion
svn export https://github.com/sdonohoo/R-Server-Spark-Azure/trunk/RStudioServer
hadoop fs -mkdir /example/data/MRSSampleData
hadoop fs -copyFromLocal /usr/lib64/microsoft-r/3.3/lib64/R/library/RevoScaleR/SampleData/* /example/data/MRSSampleData
```

Connect to the RStudio Server UI on edge node at https://CLUSTERNAME.azurehdinsight.net/rstudio/

There are two login prompts in sequence. You will first be prompted for the cluster 'admin' login you specified when creating the cluster. After providing the admin login,
you will be presented with the RStudio Server login page. Login with your sshuser id and password.

From the R console session, run the following commands to install the correct version of SparklyR for this cluster.

```R
> options(repos = "https://mran.microsoft.com/snapshot/2017-05-01")
> install.packages("sparklyr")
```

In RStudio Server UI, select "Open Project" from the File menu and open the RStudioServer.Rproj file in the ~/RStudioServer directory.


