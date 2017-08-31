#!/usr/bin/env bash
source ~/aws-cred.cfg

# ensure AWS installed
#command -v aws >/dev/null 2>&1 || { echo >&2 "AWS CLI must be installed.  Aborting."; exit 1; }

# build jar
(cd ../; sbt assembly)

# put jar in s3
aws s3 cp ../target/scala-2.10/content_reco-assembly-1.0.jar s3://thsolodata/jar/content_reco-assembly-1.0.jar

# run on EMR
#aws emr create-cluster --name NetworkQueries --ami-version 3.6 --use-default-roles --instance-type m3.xlarge --instance-count 10 \
#  --ec2-attributes KeyName=AnyMediaKey,SubnetId=subnet-067b616e --applications Name=Hive \
#   --log-uri s3://mol-spark/log \
#   --bootstrap-actions Path=s3://support.elasticmapreduce/spark/install-spark,Args=[-x,-d,spark.yarn.executor.memoryOverhead=2048] \
#   --steps Name=NetworkQueriesJob,Jar=s3://eu-west-1.elasticmapreduce/libs/script-runner/script-runner.jar,Args=[/home/hadoop/spark/bin/spark-submit,--deploy-mode,cluster,--master,yarn-cluster,--jars,s3n://mol-spark/jar/postgresql-9.4-1204.jdbc42.jar,--driver-class-path,s3n://mol-spark/jar/postgresql-9.4-1204.jdbc42.jar,--class,"scala.campaignViewability",s3n://mol-spark/jar/NetworkQueries-assembly-1.0.jar,10,$s3key,$s3secret] \
#   --auto-terminate
#

aws emr create-cluster --name "generate_features" --release-label emr-5.0.0 --applications Name=Spark --use-default-roles \
--ec2-attributes KeyName=Disso,SubnetId=subnet-fd97159a --instance-type m4.xlarge --instance-count 4 --configurations file://myConfig.json \
--log-uri s3://thsolodata/log \
--enable-debugging \
--steps Type=Spark,Name=SparkRunner,Args=[--verbose,--deploy-mode,cluster,--master,yarn-cluster,--class,scala.contentReco,s3n://thsolodata/jar/content_reco-assembly-1.0.jar,10,$s3key,$s3secret,cluster] \
--auto-terminate


# --configurations file://myConfig.json
#--vpc-id vpc-62e46f05 \
