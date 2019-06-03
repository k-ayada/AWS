#######################################################
#
#              Checks Parameter Builds
#
#   Two Roles
#   1.) circle until all parameter jobs defined
#   2.) Check EMR Until all jobs Complete
#          Will Need Work. 
#
#
import os
import json
import logging
import boto3
from boto3.dynamodb.conditions import Key
from datetime import datetime, timezone, timedelta


LOGGER = logging.getLogger("com.aws.etl")
LOGGER.setLevel(logging.INFO)
LOGGER.info('Loading function')

def lambda_handler(event, context):

    emr = boto3.client('emr')
    sns = boto3.client('sns')
    sns_topic = os.environ['sns_topic']
  
    try:
        response_cluster_search = emr.list_clusters(ClusterStates = ['WAITING'])
        LOGGER.info(response_cluster_search)
        cluster_search = len(response_cluster_search['Clusters'])
        LOGGER.info("Number of Active Clusters: " + str(cluster_search))
        if (int(cluster_search) == 0) :
            return 0
        else: 
            clusters = response_cluster_search['Clusters']
    except Exception as err:
        LOGGER.error("Error getting list of Clusters: " + str(err))

    comparison_time = datetime.now(timezone.utc) - timedelta(hours=8)
    LOGGER.info("Comparison Time :" + str(comparison_time))

    for i in clusters :
        LOGGER.info(i)
        cluster_ready_date = i['Status']['Timeline']['ReadyDateTime']
        LOGGER.info("Cluster Last Run: " + str(cluster_ready_date))
        if (comparison_time > cluster_ready_date):
            cluster_id = i['Id']
            LOGGER.info("Cluster Wait time Exceeded for :" + str(cluster_id) + " time: " + str(comparison_time))
            try: 
                cluster_timeout = emr.terminate_job_flows(JobFlowIds = [cluster_id])
                LOGGER.info("Cluster Termination Request :" + str(cluster_timeout))
                message = "EMR cluster " + cluster_id  + " is being Terminated for exceeding the wait time between jobs. "
                sns.publish(Message=message, TopicArn=sns_topic)
            except Exception as err:
                LOGGER.error("Error Terminating Cluster: " + str(err))
 
