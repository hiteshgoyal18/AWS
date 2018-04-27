from botocore.exceptions import ClientError
from botocore.vendored.requests.exceptions import ReadTimeout
from boto3.session import Session
from swf_config import timely_timespan,adVisibility_timespan,no_job_sleep_time,workflow_completed_time,init_aws_session, DOMAIN,TIMELY_WORKFLOW,TIMELY_TASKLIST
import boto3, uuid,sys,time,requests,json,traceback


"""
To start the workflow on Amazon SWF.

"""

def create_task(session,input_str):
    response = session.start_workflow_execution(
        domain=DOMAIN,
        workflowId=str(uuid.uuid4()),
        workflowType={
            'name': TIMELY_WORKFLOW,
            'version': '1.1',
        },
        taskList={
            'name': TIMELY_TASKLIST,
        },
        executionStartToCloseTimeout='7200',
        taskStartToCloseTimeout='3600',
        childPolicy='TERMINATE',
        input=input_str
    )
    if response['ResponseMetadata']['HTTPStatusCode'] == 200:
        return True


"""
Decider starts polling for the tasks :- 

"""
def poll_for_tasks(session):

    print time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))+ "Starting poll on tasklist '{}'".format(TIMELY_TASKLIST)
    response = session.poll_for_decision_task(
        domain=DOMAIN,
        taskList={
            'name': TIMELY_TASKLIST,
        },
        identity='decider-1',
    )
    return response

"""

If task is available then it checks with worker if worker is sitting idle
"""

def decider(session, response):
    if 'taskToken' not in response:
        print time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))+"Poll timed out without returning a task"
        return False

    elif 'events' in response:

        # Get Event History and Last Event
        event_history = [event for event in response['events'] if not event['eventType'].startswith('Decision')]
        last_event = event_history[-1]

        # Start once Workflow Executed
        if last_event['eventType'] == 'WorkflowExecutionStarted':

            # Get initial input
            my_input = last_event['workflowExecutionStartedEventAttributes']['input']

            # Send input to first worker
            session.respond_decision_task_completed(
                taskToken=response['taskToken'],
                decisions=[
                    {
                        'decisionType': 'ScheduleActivityTask',
                        'scheduleActivityTaskDecisionAttributes': {
                            'activityType': {
                                'name': 'start_timely_cluster',
                                'version': '1.1',
                            },
                            'activityId': 'activityid-{}'.format(str(uuid.uuid4())),
                            'control': 'Start_Cluster',
                            'scheduleToCloseTimeout': 'NONE',
                            'scheduleToStartTimeout': 'NONE',
                            'startToCloseTimeout': 'NONE',
                            'heartbeatTimeout': 'NONE',
                            'taskList': {
                                'name': TIMELY_TASKLIST,
                            },
                            'input': my_input
                        }
                    }
                ]
            )

        # Pickup and dispatch to the appropriate worker
        elif last_event['eventType'] == 'ActivityTaskCompleted':
            # Get the last activity ran
            completed_activity_id = last_event['activityTaskCompletedEventAttributes']['scheduledEventId'] - 1
            print time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))+' '+str(completed_activity_id)
            activity_data = response['events'][completed_activity_id]
            activity_attrs = activity_data['activityTaskScheduledEventAttributes']
            activity_name = activity_attrs['activityType']['name']
            # Get the result from the last activity
            result_of_first_activity = last_event['activityTaskCompletedEventAttributes'].get('result')
            print time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))+ str(result_of_first_activity)
            # Determine Next Activity
            next_activity = None
            if activity_name == 'start_timely_cluster':
                next_activity = 'add_steps'

            if next_activity is not None:
                session.respond_decision_task_completed(
                    taskToken=response['taskToken'],
                    decisions=[
                        {
                            'decisionType': 'ScheduleActivityTask',
                                'scheduleActivityTaskDecisionAttributes': {
                                    'activityType': {
                                        'name': next_activity,
                                        'version': '1.1',
                                    },
                                    'activityId': 'activityid-{}'.format(str(uuid.uuid4())),
                                    'scheduleToCloseTimeout': 'NONE',
                                    'scheduleToStartTimeout': 'NONE',
                                    'startToCloseTimeout': 'NONE',
                                    'heartbeatTimeout': 'NONE',
                                    'taskList': {
                                        'name': TIMELY_TASKLIST,
                                    },
                                    'input': result_of_first_activity
                                }
                        }
                    ]
                )
                print time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))+"Task passed to worker '{}'".format(next_activity)
            # Otherwise Complete workflow
            else:
                session.respond_decision_task_completed(
                    taskToken=response['taskToken'],
                    decisions=[
                        {
                            'decisionType': 'CompleteWorkflowExecution',
                            'completeWorkflowExecutionDecisionAttributes': {
                                'result': 'success'
                            }
                        }
                    ]
                )
                print time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))+"workflow completed"
                # if create_task(swf):
                print time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))+ ' checking pb again for further jobs'
                count=check_pb(0)
                if count==0:
                    print time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))+'cluster getting terminated'
                    terminate_cluster = client.terminate_job_flows(
                        JobFlowIds=[
                         result_of_first_activity,
                         ]
                    )
                else:
                    if create_task(swf,result_of_first_activity):
                        print time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))+" jobs available adding it on the cluster"
                        while True:
                            trigger_workflow()
                print time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))+'no new jobs...decider sleeping for half hour'
                time.sleep(workflow_completed_time)
                main()

"""
Decider checks the database if jobs are available

"""

def check_pb(counto):
    try:
        response=requests.get('https://s3-ap-southeast-1.amazonaws.com/nlplive.humanindex.data/config.json')
        data = json.loads(response.content)
        base_api_url=data['baseAPIUrl']
        version=data['version']
    except Exception:
        print 'Could not able to load json file'
        traceback.print_exc()
    param_type=['dfp','ip','session_id']
    url=base_api_url+'/'+version+'/preProcessing/GetPredictionFileJob/'+ str(timely_timespan)+'/'
    for param in param_type:
        try:
            time.sleep(1)
            api_url=url+param+'?query=true'
            fetch_response=requests.get(api_url)
            if(fetch_response.status_code==200):
                counto=1
                print time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))+' data available for '+ param
            else:
                print time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))+'status code is '+ str(fetch_response.status_code)    
        except:
            traceback.print_exc()
            pass
    try:
        time.sleep(1)
        fetch_response=requests.get(base_api_url+'/'+version+'/adVisibility/getAdVisibilityJob/'+str(adVisibility_timespan)+'?query=true')
        if(fetch_response.status_code==200):
            counto=1
        time.sleep(1)
        fetch_response=requests.get(base_api_url+'/'+version+'/model/GetPredictionJob?query=true')
        if(fetch_response.status_code==200):
            counto=1
    except:
        print time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))+ ' model/Advisibility job decider'
        traceback.print_exc()
        pass
    return counto


"""

to trigger the workflow :- 

"""
def trigger_workflow():
    try:
        response = poll_for_tasks(swf)
        decider(swf, response)
    except ReadTimeout:
        print time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))+"Read Timeout while polling..."
        pass

def main():
    print time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))+' starting new workflow'
    if create_task(swf,'abc'):
        while True:
            count=check_pb(0)
            if(count==1):
                trigger_workflow()
            else:
                print time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))+'no jobs availbale.. going down for 5 minutes..'
                time.sleep(no_job_sleep_time)



if __name__ == '__main__':
    swf = init_aws_session('swf')
    client=init_aws_session('emr')
    main()