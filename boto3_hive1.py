import boto3
import argparse
import time
import datetime

class RunEMRSteps:
    clusterids = 'xx'
    def __init__(self, args):
        self.args = args
        self.emr = boto3.client('emr', region_name=self.args['region'],aws_access_key_id="xx",
                      aws_secret_access_key="vyy")
    def clusterlist():
        response = client.list_clusters(
    CreatedAfter=datetime.datetime(2021, 5, 5),
    CreatedBefore=datetime.datetime(2021, 10, 10),
    ClusterStates=[
        'STARTING','RUNNING','WAITING',
    ]
)

        for i in response['Clusters']:
            if i['Name'] == 'xx' :
                self.clusterids=(i['Id'])
    def run(self):
        hive_cmd = 'spark-sql -f {path_to_sql}'.format(path_to_sql=self.args['path_to_sql']) \
                        if not self.args['init_file'] \
                        else 'hive -i {init_file} -f {path_to_sql}'.format(\
                            init_file=self.args['init_file'], \
                            path_to_sql=self.args['path_to_sql'])
        hive_cmd_list = hive_cmd.split()
        hive_emr_step = [
                            {
                                'Name': 'Hive_EMR_Step',
                                'ActionOnFailure': 'CONTINUE',
                                'HadoopJarStep': {
                                    'Jar': 'command-runner.jar',
                                    'Args': hive_cmd_list
                                }
                            },
                        ]
        r = self.emr.add_job_flow_steps(JobFlowId='j-1YJT3FNLUN668', Steps=hive_emr_step)
        waiter = self.emr.get_waiter('step_complete')
        waiter.wait(ClusterId='j-1YJT3FNLUN668', StepId=r['StepIds'][0],
            WaiterConfig={'Delay': 5, 'MaxAttempts': 500})
        print(time.time())

def parse_args():
    parser = argparse.ArgumentParser(description='Run Spark SQL on EMR.')
    parser.add_argument('-c', '--cluster_id', help='The cluster to run the SQL.', required=False)
    parser.add_argument('-p', '--path_to_sql', help='The SQL to be executed', required=False,default='s3://ibu-bi-etl/script/fact_ibu_ppcorder_264996.sql')
    parser.add_argument('-r', '--region', help='The region of the cluster', required=False, default='ap-southeast-1')
    parser.add_argument('-i', '--init_file', help='The file to init variables', required=False, default=None)
    parser.add_argument('-k', '--aws_key', help='', required=False,default="xxxx5")
    parser.add_argument('-s', '--aws_secret', help='', required=False,default="yyyy")
    args = vars(parser.parse_args())
    return args

def main():
    print(time.time())
    args = parse_args()
    res = RunEMRSteps(args)       
    res.run()

if __name__ == '__main__':
    main()
