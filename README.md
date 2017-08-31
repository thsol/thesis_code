# thesis
Code used to generate the initial data aggregation, model and graph input 


# Part 1 - Data Aggregation Instructions
To run this code
1. Ensure you have all the files and library dependencies within the build.sbt file
2. In terminal, locate to the repository and `sbt assembly` to build the jar
3. prod-run.sh file is calling aws-cred.cfg for ssh secret key and pass link to AWS, add this file
with your credentials to run the cluster on your AWS account
4. This cluster runs on EMR job `generate_features`
5. Before running the script, ensure you have the dataset saved in the S3 file and make changes to filenames
where necessary.

See Kaggle for dataset download:
https://www.kaggle.com/c/outbrain-click-prediction/data

Used the following datasets:-


page_views_sample.csv (433.3MB) -  original file is 2 billion, used is 10 million rows random sample data:
-	uuid,
-	document_id
-	timestamp (ms since 1970-01-01 - 1465876799998),
-	platform (desktop = 1, mobile = 2, tablet =3),
-	geo_location (country>state>DMA),
-	traffic_source (internal = 1, search = 2, social = 3)

clicks_train.csv (1.4GB) - training set, showing which of a set of ads was clicked:
-	display_id
-	ad_id

-	clicked (1 if clicked, 0 otherwise)
clicks_test.csv (483.5MB) - test set, showing which of a set of ads was clicked.
-	display_id
-	ad_id

events.csv (1.1GB) - information on the display_id context. It covers both the train and test set:
-	display_id
-	uuid
-	document_id
-	timestamp
-	platform
-	geo_location

promoted_content.csv (13.2MB) - details on the ads:
-	ad_id
-	document_id
-	campaign_id
-	advertiser_id


# Part 2 - Graph Input Instructions

1. gephi_input.py takes output from Part 1 and loads it as a numpy array to strip elements for graph analysis
outputting edges.csv and nodes.csv for input in Gephi tool
2. graph_input.py generates the log-log graph plot for node degree analysis



The cluster should take 15 minutes to run and output a file into your s3 folder.