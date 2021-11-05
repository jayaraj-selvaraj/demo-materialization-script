# demo-materialization-script
Data team's basic need is to extract data from different source endpoints(RDBMS/File/Hadoop...) and send it to different target endpoints such as Cloud(GCP/Azure/AWS/Hadoop). To acheive this basic functionality and make it repeatable pattern for ingestion an "End to End data ingestion suite" has been built which is driven by metadata/UI. Data pipeline steps are seperated and Each of them are a simple microservice that can be plugged together to create a data pipeline.

1.Read/Extract data from source 
-- Apache Nifi data flow tool in cluster mode is used to extract the data (managed via Rest)
2.Stage data in transient storage 
-- Shared NAS
3.Write data to target (Read once and write multiple targets) 
-- Apache Nifi is used to write to target
4.Materialize/Publish (Optional for Hadoop/GCP target) 
-- Create Hive/BQ tables if user needs else this step is optional
5.Reconcile data between source and target 
-- Make sure the process sent the data wiothout any loss and capture the details of it
6.Delete data from transient/stage storage 
-- Remove all temp data from transient NAS after successful movememnt and reconciliation


This piece of code in repo is part of Step 4 and is used to prepare Hive DDLS based on user configuration per feed and create Hive abstract on the data ingested in to HDFS area by earlier steps.
