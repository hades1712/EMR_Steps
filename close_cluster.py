import boto3
import datetime

client = boto3.client("emr")


def listclusters():
    response =client.list_clusters(
        CreatedAfter=datetime.datetime(2021, 5, 5),
        CreatedBefore=datetime.datetime(2021, 10, 10),
        ClusterStates=[
            'RUNNING','WAITING',
        ]
    )
    for i in response['Clusters']:
        if i['Name'] != 'ibu-bi-etl-cluster3' :
            pass
        else:
            clusterids= i['Id']
            break
    return clusterids

try:
    clusterids1 = listclusters()
except:
    print("EMR Cluster not exist")
    exit(1)


response1 = client.terminate_job_flows(
        JobFlowIds=[
            clusterids1,
        ]
    )

waiter = client.get_waiter('cluster_terminated')
waiter.wait(
    ClusterId=clusterids1,
    WaiterConfig={
        'Delay': 100,
        'MaxAttempts': 100
    }
)
