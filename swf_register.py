from botocore.exceptions import ClientError
from swf_config import init_aws_session, DOMAIN, DAILY_WORKFLOW,TIMELY_WORKFLOW
import boto3

def register_domain(session):
    try:
        response = session.register_domain(
            name=DOMAIN,
            description='Testing domain for learning SWF',
            workflowExecutionRetentionPeriodInDays='30',
        )
        print "[*] Registered domain '{}' successfully".format(DOMAIN)
    except ClientError:
        print "[*] Domain '{}' is already registered".format(DOMAIN)

def register_workflow(session):
    try:
        response = session.register_workflow_type(
            domain=DOMAIN,
            name=DAILY_WORKFLOW,
            version='1.1',
            description='HI workflow manager',
        )
        print "[*] Registered workflow type '{}'".format(DAILY_WORKFLOW)
    except ClientError:
        print "[*] Workflow Type '{}' is already registered".format(DAILY_WORKFLOW)

    try:
        response = session.register_workflow_type(
            domain=DOMAIN,
            name=TIMELY_WORKFLOW,
            version='1.1',
            description='HI workflow manager',
        )
        print "[*] Registered workflow type '{}'".format(TIMELY_WORKFLOW)
    except ClientError:
        print "[*] Workflow Type '{}' is already registered".format(TIMELY_WORKFLOW)

def register_activities(session):
    activities = ['start_timely_cluster', 'add_steps','start_daily_cluster']
    for act in activities:
        try:
            response = session.register_activity_type(
                domain=DOMAIN,
                name=act,
                version='1.1',
            )
            print "[*] Registered activity type '{}'".format(act)
        except ClientError:
            print "[*] Activity Type '{}' already registered".format(act)

if __name__ == '__main__':
    session = init_aws_session('swf')
    register_domain(session)
    register_workflow(session)
    register_activities(session)