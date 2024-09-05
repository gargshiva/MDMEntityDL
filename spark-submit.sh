spark-submit --master yarn --deploy-mode cluster \
--py-files sbdl.zip \
--files conf/app.conf,conf/spark.conf,log4j2.properties \
--driver-cores 2 \
--driver-memory 3G \
--conf spark.driver.memoryOverhead=1G \
 main.py PROD s3://emr-tutorial-s3-bucket/input/accounts/account_samples.csv s3://emr-tutorial-s3-bucket/input/parties/party_samples.csv s3://emr-tutorial-s3-bucket/input/party_addresses/address_samples.csv s3://emr-tutorial-s3-bucket/output/