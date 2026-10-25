"""
Lambda: taxi-start-pipeline   (python3.12, timeout 60 s, trigger = SQS taxi-file-events, batch 10)

new file raw/<feed>/...  ->  feed name = 2nd folder  ->  config exists?  ->  start Step Functions
Replace <ACCOUNT_ID> with your 12-digit AWS account id before deploying.
"""
import json, boto3
s3, sfn = boto3.client("s3"), boto3.client("stepfunctions")
BUCKET = "faiz-taxi-lake-2026"
SFN_ARN = "arn:aws:states:ap-southeast-2:<ACCOUNT_ID>:stateMachine:taxi-pipeline"

def lambda_handler(event, context):
    feeds = set()                                     # same feed many files -> run once
    for msg in event["Records"]:                      # each SQS message
        body = json.loads(msg["body"])
        for rec in body.get("Records", []):           # each S3 file (skips AWS test events)
            key = rec["s3"]["object"]["key"]          # e.g. raw/green_trips/year=2024/...
            feeds.add(key.split("/")[1])              # -> green_trips
    for feed in feeds:
        try:
            s3.head_object(Bucket=BUCKET, Key=f"config/feeds/{feed}.json")   # is there a form?
        except Exception:
            print(f"No config for '{feed}', skipping"); continue
        sfn.start_execution(stateMachineArn=SFN_ARN, input=json.dumps({"feed": feed}))
        print(f"Started pipeline for {feed}")
