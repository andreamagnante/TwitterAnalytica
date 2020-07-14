# TwitterAnalytica
Project for the cloud computing course. Analysis of tweets streams through Apache Spark and visualization on interface realized in Streamlit

## Authors

* [Bianca Francesco](https://github.com/francescobianca/)
* [Magnante Andrea](https://github.com/andreamagnante)
* [Prandini Manuel](https://github.com/ManuelPrandini)
* Manduca Lorenzo

![GitHub Logo](https://github.com/francescobianca/TwitterAnalytica/blob/master/images/architecture.png)
![GitHub Logo](https://github.com/francescobianca/TwitterAnalytica/blob/master/images/monitor1.png)
![GitHub Logo](https://github.com/francescobianca/TwitterAnalytica/blob/master/images/monitor2.png)
![GitHub Logo](https://github.com/francescobianca/TwitterAnalytica/blob/master/images/smartphone.png)

## Twitter Rest API

The Twitter API continuously listens to the tweets stream.

## Amazon Kinesis Firehose

The stream is sent to Kinesis who manages its saving on S3.

## Amazon Glue + Parquet

Amazon Glue organizes the flow into structured data using Parquet

## Amazon S3

S3 saves the data collected in buckets organized by days.

## Amazon EMR with Apache PySpark

The EMR cluster takes care of running Spark and processing the data.

## Streamlit UI 

The interface displays the data and topics sought by the user interacting with the cluster through the Apache Livy Rest API
