import json
import pulumi
import pulumi_aws as aws

#Configs
STAGE_BUCKET = 'udend-moshe'
current = aws.get_caller_identity()

bucket = aws.s3.Bucket(STAGE_BUCKET)
bucket_policy = aws.s3.BucketPolicy(STAGE_BUCKET+'_policy',
    bucket=bucket.id,
    policy=bucket.arn.apply(
        lambda bucket_arn: json.dumps({
            "Version": "2012-10-17",
            "Id": STAGE_BUCKET+'_policy',
            "Statement": [{
                'Sid': "GrantS3Access",
                'Effect': "Allow",
                'Action': [
                    "s3:*"
                ],
                "Principal": {
                    "Service": "emr-containers.amazonaws.com",
                    "AWS": current.account_id
                },
                "Resource": [
                    bucket_arn,
                    f"{bucket_arn}/*",
                ],
            }],
        })))

pulumi.export('bucket', bucket.arn)