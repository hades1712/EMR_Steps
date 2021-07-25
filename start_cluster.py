import boto3
import datetime
aws_access_key = "xxxxxx"
aws_secret_key = "yyyyyy"
region_name = "region_name"
emr_log_url = "s3:S3_URL"
client = boto3.client("emr", region_name=region_name, aws_access_key_id=aws_access_key,
                      aws_secret_access_key=aws_secret_key)

response =client.list_clusters(
    CreatedAfter=datetime.datetime(2021, 5, 5),
    CreatedBefore=datetime.datetime(2021, 10, 10),
    ClusterStates=[
        'RUNNING','WAITING',
    ]
)
for i in response['Clusters']:
    if i['Name'] != 'cluster_name' :
        pass
    else :
        print("The EMR Cluster Already Exits")
        exit(1)

response = client.run_job_flow(
    Name='cluster_name',   # [Required] clustername
    LogUri=emr_log_url,          # 日志位置（s3）桶
    ReleaseLabel='emr-5.33.0',     #  [Required]emr 版本
    Instances={
        'InstanceGroups': [
            {
                'Name': 'test2',
                'Market': 'ON_DEMAND',
                'InstanceRole': 'MASTER' ,
                'InstanceType': 'm5.2xlarge',
                'InstanceCount': 1,
            },
            {
                'Name': 'test2',
                'Market': 'SPOT',
                'InstanceRole': 'CORE' ,
                'InstanceType': 'r6gd.8xlarge',
                'InstanceCount': 2,
            },
        ],
        'Ec2KeyName': 'key_paires',    #keyname
        'KeepJobFlowAliveWhenNoSteps': True,        #【Required】 如果没有steps ，集群继续运行
        'TerminationProtected': False,              # [Required] 关闭终止保护
        'EmrManagedMasterSecurityGroup': 'sg-xxxx',     #建议指定securitygroup （先手动新创建集群，然后用手动创建集群的sg）
        'EmrManagedSlaveSecurityGroup': 'sg-yyyyy',         #建议指定securitygroup （先手动新创建集群，然后用手动创建集群的sg） 
        #'Ec2SubnetId': 'subnet-dc278185' ,                            #建议指定相应的subnet
        },
    Applications=[
        {                                   #启动的application及版本
            'Name': "hive"
        },
        {
            'Name':'spark'
        }
            ],
    Configurations = [
            {
                'Classification': 'hive-site',
                'Properties': {
                    'hive.metastore.client.factory.class': 'com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory'
                }
            },
                    {
                'Classification': 'spark-hive-site',
                'Properties': {
                    'hive.metastore.client.factory.class': 'com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory'
                }
            }
        ],   
    Tags =[                                           #建议为集群打上相应的tag 便于未来管理
        {
            'Key': 'Key_name',
            'Value': 'value'
        }
    ],
    JobFlowRole='EMR_EC2_DefaultRole',             #Required 使用默认的权限角色 
    ServiceRole='EMR_DefaultRole',                  #Required 使用默认的权限角色
    VisibleToAllUsers=True                          #Required 集群对其他用户可见
    
)
    
waiter = client.get_waiter('cluster_running')
try:
    waiter.wait(
        ClusterId=response["JobFlowId"],
        WaiterConfig={
            'Delay': 5,
            'MaxAttempts': 50
        }
    )
except:
    response = client.describe_cluster(
    ClusterId= response["JobFlowId"]
)