#!/usr/bin/env bash
source ~/aws-cred.cfg

# ensure AWS installed
command -v aws >/dev/null 2>&1 || { echo >&2 "AWS CLI must be installed.  Aborting."; exit 1; }


# build jar
(cd ../; sbt assembly)


# run locally
$sparkdir/bin/spark-submit \
--class "scala.logisticRegression" \
--master local[4] \
../target/scala-2.10/content_reco-assembly-1.0.jar local $s3key $s3secret