#!/usr/bin/env bash
# ------------------------------------------------------------------
# NYC Taxi Data Lake on AWS - the CloudShell commands, in order.
#
# Run in AWS CloudShell (region: Sydney, ap-southeast-2) from the repo root.
# Paste one block at a time and check the output before the next block.
# No secrets here. The account id is read from AWS at run time.
#
# Done in the console (not here): budgets ($35 + zero-spend), the two
# Glue jobs (the CLI version is below if you prefer), clicking
# "Confirm subscription" in the SNS email.
# ------------------------------------------------------------------
set -euo pipefail

REGION=ap-southeast-2
BUCKET=faiz-taxi-lake-2026
ALERT_EMAIL="${ALERT_EMAIL:?run as: ALERT_EMAIL=you@example.com bash infra/setup.sh}"
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
export AWS_DEFAULT_REGION=$REGION

# ---------- Step 2: bucket + 20M rows of raw data ----------
aws s3 mb s3://$BUCKET --region $REGION
for m in 01 02 03 04 05 06; do
  curl -sSfO https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-$m.parquet
  aws s3 cp yellow_tripdata_2024-$m.parquet s3://$BUCKET/raw/yellow_trips/year=2024/month=$m/
  rm yellow_tripdata_2024-$m.parquet
done
curl -sSfO https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
aws s3 cp taxi_zone_lookup.csv s3://$BUCKET/raw/taxi_zones/
aws s3 ls s3://$BUCKET/raw/ --recursive --human-readable --summarize

# ---------- Step 4a: Glue role (only Glue can use it, only this bucket) ----------
aws iam create-role --role-name taxi-glue-role \
  --assume-role-policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"glue.amazonaws.com"},"Action":"sts:AssumeRole"}]}' > /dev/null
aws iam attach-role-policy --role-name taxi-glue-role \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole
aws iam put-role-policy --role-name taxi-glue-role --policy-name taxi-bucket-access \
  --policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:GetObject","s3:PutObject","s3:DeleteObject","s3:ListBucket"],"Resource":["arn:aws:s3:::'$BUCKET'","arn:aws:s3:::'$BUCKET'/*"]}]}'

# ---------- Step 4b + 6b: Glue jobs (CLI version of what we did in the console) ----------
aws s3 cp glue/ s3://$BUCKET/scripts/ --recursive
sleep 10   # IAM needs a few seconds
aws glue create-job --name taxi-raw-to-clean \
  --role arn:aws:iam::$ACCOUNT_ID:role/taxi-glue-role \
  --worker-type G.1X --number-of-workers 3 --timeout 30 \
  --command "Name=glueetl,ScriptLocation=s3://$BUCKET/scripts/taxi_raw_to_clean.py,PythonVersion=3" \
  --default-arguments '{"--BUCKET":"s3://'$BUCKET'","--MONTH":"all"}' > /dev/null
aws glue create-job --name taxi-feed-framework \
  --role arn:aws:iam::$ACCOUNT_ID:role/taxi-glue-role \
  --worker-type G.1X --number-of-workers 2 --timeout 20 \
  --command "Name=glueetl,ScriptLocation=s3://$BUCKET/scripts/taxi_feed_framework.py,PythonVersion=3" \
  --default-arguments '{"--BUCKET":"'$BUCKET'","--FEED":"taxi_zones"}' > /dev/null

# ---------- Step 6a + 6c: feed configs + the green taxi feed ----------
aws s3 cp configs/feeds/ s3://$BUCKET/config/feeds/ --recursive --exclude "broken_feed.json"
curl -sSfO https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2024-01.parquet
aws s3 cp green_tripdata_2024-01.parquet s3://$BUCKET/raw/green_trips/year=2024/month=01/
rm green_tripdata_2024-01.parquet

# ---------- Step 7a: SNS email alarm ----------
aws sns create-topic --name taxi-pipeline-alerts > /dev/null
aws sns subscribe --protocol email --notification-endpoint "$ALERT_EMAIL" \
  --topic-arn arn:aws:sns:$REGION:$ACCOUNT_ID:taxi-pipeline-alerts

# ---------- Step 7a: SQS waiting line (only our bucket may send) ----------
cat > attrs.json <<EOF
{
 "VisibilityTimeout": "300",
 "Policy": "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":{\"Service\":\"s3.amazonaws.com\"},\"Action\":\"sqs:SendMessage\",\"Resource\":\"arn:aws:sqs:$REGION:$ACCOUNT_ID:taxi-file-events\",\"Condition\":{\"ArnLike\":{\"aws:SourceArn\":\"arn:aws:s3:::$BUCKET\"}}}]}"
}
EOF
aws sqs create-queue --queue-name taxi-file-events --attributes file://attrs.json

# ---------- Step 7a: S3 -> SQS for every new file under raw/ ----------
cat > notif.json <<EOF
{"QueueConfigurations":[{
  "QueueArn":"arn:aws:sqs:$REGION:$ACCOUNT_ID:taxi-file-events",
  "Events":["s3:ObjectCreated:*"],
  "Filter":{"Key":{"FilterRules":[{"Name":"prefix","Value":"raw/"}]}}}]}
EOF
aws s3api put-bucket-notification-configuration --bucket $BUCKET \
  --notification-configuration file://notif.json

# ---------- Step 7b: Step Functions role + state machine ----------
aws iam create-role --role-name taxi-sfn-role --assume-role-policy-document \
 '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"states.amazonaws.com"},"Action":"sts:AssumeRole"}]}' > /dev/null
aws iam put-role-policy --role-name taxi-sfn-role --policy-name taxi-sfn-access --policy-document \
 '{"Version":"2012-10-17","Statement":[
   {"Effect":"Allow","Action":["glue:StartJobRun","glue:GetJobRun","glue:GetJobRuns","glue:BatchStopJobRun"],"Resource":"*"},
   {"Effect":"Allow","Action":"sns:Publish","Resource":"arn:aws:sns:'$REGION':'$ACCOUNT_ID':taxi-pipeline-alerts"}]}'
sed "s/<ACCOUNT_ID>/$ACCOUNT_ID/g" stepfunctions/taxi-pipeline.asl.json > sfn.json
sleep 10
aws stepfunctions create-state-machine --name taxi-pipeline \
  --role-arn arn:aws:iam::$ACCOUNT_ID:role/taxi-sfn-role \
  --definition file://sfn.json > /dev/null

# ---------- Step 7c: Lambda role + function + SQS trigger ----------
aws iam create-role --role-name taxi-lambda-role --assume-role-policy-document \
 '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}' > /dev/null
aws iam attach-role-policy --role-name taxi-lambda-role \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaSQSQueueExecutionRole
aws iam put-role-policy --role-name taxi-lambda-role --policy-name taxi-lambda-access --policy-document \
 '{"Version":"2012-10-17","Statement":[
   {"Effect":"Allow","Action":"states:StartExecution","Resource":"arn:aws:states:'$REGION':'$ACCOUNT_ID':stateMachine:taxi-pipeline"},
   {"Effect":"Allow","Action":"s3:GetObject","Resource":"arn:aws:s3:::'$BUCKET'/config/*"}]}'
sed "s/<ACCOUNT_ID>/$ACCOUNT_ID/g" lambda/lambda_function.py > lambda_function.py
zip -q lambda.zip lambda_function.py
sleep 10
aws lambda create-function --function-name taxi-start-pipeline --runtime python3.12 \
  --handler lambda_function.lambda_handler --zip-file fileb://lambda.zip --timeout 60 \
  --role arn:aws:iam::$ACCOUNT_ID:role/taxi-lambda-role > /dev/null
aws sqs purge-queue --queue-url https://sqs.$REGION.amazonaws.com/$ACCOUNT_ID/taxi-file-events
aws lambda create-event-source-mapping --function-name taxi-start-pipeline --batch-size 10 \
  --event-source-arn arn:aws:sqs:$REGION:$ACCOUNT_ID:taxi-file-events > /dev/null

rm -f attrs.json notif.json sfn.json lambda_function.py lambda.zip taxi_zone_lookup.csv
echo "ALL SET. Confirm the SNS email, then run sql/01_create_tables.sql in Athena."
