import json
import pulumi
import pulumi_aws as aws
import requests

#Configs
STAGE_BUCKET = 'udend-moshe'
current = aws.get_caller_identity()

r = requests.get(r'http://jsonip.com')
CURRENT_MACHINE_IP = r.json()['ip']


## Create bucket for data and logs
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


# create vpc for EMR cluster

# Networking
# ----------------------------------------------------------------------------------------------------------------------

#Networking
CURRENT_USER = 'moshe'
VPC_NAME = CURRENT_USER + "-VPC-Pulumi"
SUBNET_NAME = CURRENT_USER + "-Subnet-Pulumi"
GATEWAY_NAME = CURRENT_USER + "-GW-Pulumi"
ROUTE_TABLE_NAME = CURRENT_USER + "-Route-Pulumi"
MAIN_ROUTE_TABLE_ASSOCIATION_NAME = CURRENT_USER + "-Main-RouteAssociation-Pulumi"
SECURITY_GROUP_NAME = CURRENT_USER + "-SecurityGroup-Pulumi"
TAGS = {"user": CURRENT_USER, "tool": "Pulumi", "purpose": "Spark_TPCDS_Benchmark"}

vpc = aws.ec2.Vpc(VPC_NAME,
                  cidr_block="173.31.0.0/16",
                  enable_dns_hostnames=True,
                  tags=TAGS)

subnet = aws.ec2.Subnet(SUBNET_NAME,
                        vpc_id=vpc.id,
                        cidr_block="173.31.0.0/20",
                        availability_zone="ap-south-1b",
                        tags=TAGS)

# Enable SSH login from the user machine by default
security_group = aws.ec2.SecurityGroup(resource_name=SECURITY_GROUP_NAME,
                                       description="Allow inbound SSH traffic",
                                       vpc_id=vpc.id,
                                       ingress=[aws.ec2.SecurityGroupIngressArgs(
                                           from_port=22,
                                           to_port=22,
                                           protocol="TCP",
                                           cidr_blocks=[CURRENT_MACHINE_IP+"/32"]
                                           # Only the current user machine is allowed to login by default
                                       )],
                                       tags=TAGS,
                                       opts=pulumi.ResourceOptions(depends_on=[subnet]))


gate_way = aws.ec2.InternetGateway(GATEWAY_NAME, vpc_id=vpc.id, tags=TAGS)
route_table = aws.ec2.RouteTable(ROUTE_TABLE_NAME,
                                 tags=TAGS,
                                 vpc_id=vpc.id,
                                 routes=[aws.ec2.RouteTableRouteArgs(
                                     cidr_block="0.0.0.0/0",
                                     gateway_id=gate_way.id,
                                 )])

main_route_table_association = aws.ec2.MainRouteTableAssociation(MAIN_ROUTE_TABLE_ASSOCIATION_NAME,
                                                                 vpc_id=vpc.id,
                                                                 route_table_id=route_table.id)

pulumi.export('bucket', bucket.arn)