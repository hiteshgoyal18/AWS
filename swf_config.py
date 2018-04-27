from botocore.client import Config
from boto3.session import Session

def init_aws_session(service):
  access_key_id = 'access_key_id'
  secret_access_key = 'secret access key'
    # Create Session
    session = Session(
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
        region_name='ap-southeast-1',
    )
    client = session.client(service)
    return client

# Global Configuration Settings

DOMAIN = 'domain_name'
TIMELY_WORKFLOW = 'timelyWorkflow'
DAILY_WORKFLOW = 'dailyWorkflow'
DAILY_TASKLIST = 'dailyTasklist'
TIMELY_TASKLIST='timelyTasklist'


# Update Timeouts
config = Config(connect_timeout=70, read_timeout=70)
no_job_sleep_time=300
workflow_completed_time=1800
timely_timespan=30
adVisibility_timespan=1

daily_master_cluster_instance_type="m2.2xlarge"
daily_master_cluster_instance_count=1
daily_slave_cluster_instance_type="c3.4xlarge"
daily_slave_cluster_instance_count=2

"""
Configuration of EMR cluster ......
"""

cluster_config={
  "logUri": "s3://aws-logs-993183203787-ap-southeast-1/elasticmapreduce",
  "releaseLabel": "emr-5.4.0",
  "applications": [
    {
      "Name": "Spark"
    },
    {
      "Name": "Hadoop"
    },
    {
      "Name": "Hue"
    },
    {
      "Name": "Ganglia"
    },
    {
      "Name": "Hive"
    }
  ],
  "visibleToAllUser": True,
  "jobFlowRole": "EMR_EC2_DefaultRole",
  "serviceRole": "EMR_DefaultRole",
  "instances": {
    "InstanceGroups": [
      {
        "Name": "Master nodes",
        "Market": "ON_DEMAND",
        "InstanceRole": "MASTER",
        "InstanceType": "m3.xlarge",
        "InstanceCount": 1
      },
      {
        "Name": "Slave nodes",
        "Market": "ON_DEMAND",
        "InstanceRole": "CORE",
        "InstanceType": "c3.xlarge",
        "InstanceCount": 1
      }
    ],
    "Ec2KeyName": "aws_spark",
    "KeepJobFlowAliveWhenNoSteps": True,
    "TerminationProtected": False
  },
"copyFileStep": {
    "Name": "copy file",
    "ActionOnFailure": "CONTINUE",
    "HadoopJarStep": {
      "Jar": "command-runner.jar",
      "Args": [
        "aws",
        "s3",
        "cp",
        "bucket_path_code",
        "/home/hadoop/"
      ]
    }
  }
}