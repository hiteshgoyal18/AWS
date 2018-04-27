from botocore.exceptions import ClientError
from botocore.vendored.requests.exceptions import ReadTimeout
from boto3.session import Session
from swf_config import init_aws_session, DOMAIN,TIMELY_TASKLIST,cluster_config,timely_timespan,adVisibility_timespan
import boto3, uuid,time,sys,requests,json,traceback


"""
Starts the cluster for execution of jobs
"""

def start_timely_cluster(text):
    cluster_id=''
    status=0
    if(text=='abc'):
        cluster = connection.run_job_flow(
            Name='timely_file_cluster',
            LogUri=cluster_config['logUri'],
            ReleaseLabel=cluster_config['releaseLabel'],
            Applications=cluster_config['applications'],
            Instances=cluster_config['instances'],
            Steps=[
            ],
            VisibleToAllUsers=cluster_config['visibleToAllUser'],
            JobFlowRole=cluster_config['jobFlowRole'],
            ServiceRole=cluster_config['serviceRole'],
        )
        print time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))+'starting cluster '+cluster['JobFlowId']
        cluster_status=connection.describe_cluster(ClusterId=cluster['JobFlowId'])
        while(cluster_status['Cluster']['Status']['State']!="WAITING"):
            if(cluster_status['Cluster']['Status']['State']=="TERMINATED_WITH_ERRORS"):
                print 'cluster terminated with errors... restarting new'
                status=1
                break
            print time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))+' cluster is getting started'
            time.sleep(30)
            cluster_status=connection.describe_cluster(ClusterId=cluster['JobFlowId'])
        if(status==1):
            start_timely_cluster('abc')
        cluster_id=cluster['JobFlowId']   
    else:
        cluster_id=text

    return cluster_id


"""
adds/submit jobs on Spark Cluster for execution.

"""

def add_steps(clusterId):
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
    runner_step={
    'ActionOnFailure': 'CONTINUE',
    'HadoopJarStep': {
        'Jar': 'command-runner.jar',
        'Args': [
            'spark-submit',
            '/home/hadoop/',
            str(timely_timespan)
        ]
    }
    }
    try:
        fetch_response=requests.get(base_api_url+'/'+version+'/model/GetPredictionJob?query=true')
        if(fetch_response.status_code==200):
            print time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))+' data available for model prediction job'
            file_copy=cluster_config['copyFileStep']
            file_copy['HadoopJarStep']['Args'][3]="s3://nlplive.humanindex.data/codes/pb_integrated_complete_predictions.py"
            file_run=runner_step
            file_run['Name']="Prediction Preprocessing"
            file_run['HadoopJarStep']['Args'][1]="/home/hadoop/pb_integrated_complete_predictions.py"
            file_run['HadoopJarStep']['Args'][2]=""
            response=connection.add_job_flow_steps(
            JobFlowId=clusterId,
            Steps=[
                    file_copy,
                    file_run
            ]
            )
    except:
        pass

    for param in param_type:
        try:
            api_url=url+param+'?query=true'
            fetch_response=requests.get(api_url)
            if(fetch_response.status_code==200):
                print time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))+' data available for '+param+' file preprocessing'
                file_copy=cluster_config['copyFileStep']
                file_copy['HadoopJarStep']['Args'][3]="s3://nlplive.humanindex.data/codes/pb_integrated_complete_timed_preprocessing_"+param+".py"
                file_run=runner_step
                file_run['Name']= param+" timely preProcessing"
                file_run['HadoopJarStep']['Args'][1]="/home/hadoop/pb_integrated_complete_timed_preprocessing_"+param+".py"
                file_run['HadoopJarStep']['Args'][2]=str(timely_timespan)
                response = connection.add_job_flow_steps(
                JobFlowId=clusterId,
                Steps=[
                   file_copy,
                   file_run
                    ]
                )   
        except:
            pass

    try:
        fetch_response=requests.get(base_api_url+'/'+version+'/adVisibility/getAdVisibilityJob/'+str(adVisibility_timespan)+'?query=true')
        if(fetch_response.status_code==200):
            print time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))+'data available for ad visibility'
            file_copy=cluster_config['copyFileStep']
            file_copy['HadoopJarStep']['Args'][3]='s3://nlplive.humanindex.data/codes/adVisibility.py'
            file_run=runner_step
            file_run['Name']='Advisiblity Processing Job'
            file_run['HadoopJarStep']['Args'][1]="/home/hadoop/adVisibility.py"
            file_run['HadoopJarStep']['Args'][2]=str(adVisibility_timespan)
            response=connection.add_job_flow_steps(
        	JobFlowId=clusterId,
            Steps=[
                    file_copy,
                    file_run
            ]
            )
    except:
        pass

    print time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))+' steps added'
    time.sleep(30)
    cluster_status=connection.describe_cluster(ClusterId=clusterId)
    while(cluster_status['Cluster']['Status']['State']!="WAITING"):
        print time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))+' Steps are running'
        time.sleep(30)
        cluster_status=connection.describe_cluster(ClusterId=clusterId) 
    return clusterId


"""
If worker gets idle it starts polling to get the tasks assigned
"""

def poll_for_tasks(session):
    print time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))+" Polling for activity tasks on '{}'".format(TIMELY_TASKLIST)
    task = session.poll_for_activity_task(
        domain=DOMAIN,
        taskList={
            'name': TIMELY_TASKLIST,
        },
        identity='preprocessing_adVisibility_prediction_worker'
    )

    if 'taskToken' not in task:
        print time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))+" Poll timed out no new task"

    else:
        # Define a map of our activities to methods/funcs
        if 'activityId' in task:
            activity = {
                'start_timely_cluster': start_timely_cluster,
                'add_steps': add_steps,
            }.get(task['activityType']['name'])
            new_string = activity(task.get('input'))
            # Let the decider know we are done with said task
            session.respond_activity_task_completed(
                taskToken=task['taskToken'],
                result=new_string
            )
            print task['taskToken']
            print new_string
        else:
            print "Unable to find activityId"


if __name__ == '__main__':
    swf = init_aws_session('swf')
    connection=init_aws_session('emr')
    while True:
        try:
            poll_for_tasks(swf)
        except ReadTimeout:
            pass