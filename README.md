# Instructions
This file contains all the necessary instructions to run this excercise.

## Steps
First of all you will need the tools to run the code. The first tool is the [Cloudera Quickstart VM](https://www.cloudera.com/downloads/quickstart_vms/5-13.html), this virtual machine containes two of the tools
that we are going to use, these are Hadoop and Spark.
1. With Hadoop we save the main file with the data.
2. With Spark we retrieve the most important data from the data source, in this case we tried to get the best wines in the dataset, relationship between price and points and the best tasters.

Second and not less important you need to install SBT which is the tool to turn the scala file to .jar and then execute it inside Spark with the follow command spark-submit <Name of the .jar file> handler.Handler

It is important that the build.sbt dependency that contains spark is with the same version that the Spark software, in my case 1.6.0

## Source
The data comes from Kaggle a [wine-review](https://www.kaggle.com/zynicide/wine-reviews) dataset.
