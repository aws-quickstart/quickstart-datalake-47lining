from __future__ import print_function

import base64
import json
import os
import urllib.parse

import boto3
from aws_requests_auth.aws_auth import AWSRequestsAuth
from botocore.exceptions import ClientError
from botocore.vendored import requests
from elasticsearch import Elasticsearch, RequestsHttpConnection, ElasticsearchException

CFN_SUCCESS = 'SUCCESS'
CFN_FAILED = 'FAILED'
TMP_KIBANA_JSON_PATH = '/tmp/kibana_dashboards.json'

s3 = boto3.client('s3')
s3_resource = boto3.resource('s3')
es_index = 'metadata'


def make_elasticsearch_client(elasticsearch_endpoint):
    awsauth = AWSRequestsAuth(
        aws_access_key=os.environ['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
        aws_token=os.environ['AWS_SESSION_TOKEN'],
        aws_host=elasticsearch_endpoint,
        aws_region=os.environ['AWS_REGION'],
        aws_service='es'
    )
    return Elasticsearch(
        hosts=['{0}:443'.format(elasticsearch_endpoint)],
        use_ssl=True,
        connection_class=RequestsHttpConnection,
        http_auth=awsauth
    )


def handle_bucket_event(event, context):
    sns_message = json.loads(event["Records"][0]["Sns"]["Message"])
    bucket = sns_message["Records"][0]["s3"]["bucket"]["name"]
    key = urllib.parse.unquote_plus(sns_message["Records"][0]["s3"]["object"]["key"])
    print(bucket, key)
    try:
        response = s3.head_object(Bucket=bucket, Key=key)
    except ClientError as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist, your bucket is in the same region as this function and necessary permissions have been granted.'.format(key, bucket))
        raise e

    metadata = {
        'key': key,
        'ContentLength': response['ContentLength'],
        'SizeMiB': response['ContentLength'] / 1024**2,
        'LastModified': response['LastModified'].isoformat(),
        'ContentType': response['ContentType'],
        'ETag': response['ETag'],
        'Dataset': key.split('/')[0]
    }
    print("METADATA: " + str(metadata))

    es_client = make_elasticsearch_client(os.environ['ELASTICSEARCH_ENDPOINT'])

    try:
        es_client.index(index=es_index, doc_type=bucket, body=json.dumps(metadata))
    except ElasticsearchException as e:
        print(e)
        print("Could not index in Elasticsearch")
        raise e


def create_metadata_visualizations(elasticsearch_endpoint):
    es_client = make_elasticsearch_client(elasticsearch_endpoint)
    es_client.index(index='.kibana', doc_type='config', id='5.1.1', body=json.dumps({'defaultIndex': 'metadata'}))
    data = {'title': 'metadata', 'timeFieldName': 'LastModified'}
    es_client.index(index='.kibana', doc_type='index-pattern', id='metadata', body=json.dumps(data))
    with open(TMP_KIBANA_JSON_PATH) as visualizations_file:
        visualizations = json.load(visualizations_file)
    for visualization in visualizations:
        es_client.index(
            index='.kibana',
            doc_type=visualization['_type'],
            id=visualization['_id'],
            body=json.dumps(visualization['_source'])
        )


def register_metadata_dashboard(event, context):
    if event['RequestType'] != 'Create':
        return send_cfnresponse(event, context, CFN_SUCCESS, {})
    quickstart_bucket = s3_resource.Bucket(event['ResourceProperties']['QSS3BucketName'])
    kibana_dashboards_key = os.path.join(
        event['ResourceProperties']['QSS3KeyPrefix'],
        'assets/kibana/kibana_metadata_visualizations.json'
    )
    elasticsearch_endpoint = event['ResourceProperties']['ElasticsearchEndpoint']
    try:
        quickstart_bucket.download_file(kibana_dashboards_key, TMP_KIBANA_JSON_PATH)
        create_metadata_visualizations(elasticsearch_endpoint)
        return send_cfnresponse(event, context, CFN_SUCCESS, {})
    except (ClientError, ElasticsearchException) as e:
        print(e)
        return send_cfnresponse(event, context, CFN_FAILED, {})


def send_cfnresponse(event, context, response_status, data: dict):
    response_url = event['ResponseURL']
    print('CFN response url: {}'.format(response_url))

    response_body = {}
    response_body['Status'] = response_status
    response_body['Reason'] = 'See the details in CloudWatch Log Stream: ' + context.log_stream_name
    response_body['PhysicalResourceId'] = context.log_stream_name
    response_body['StackId'] = event['StackId']
    response_body['RequestId'] = event['RequestId']
    response_body['LogicalResourceId'] = event['LogicalResourceId']
    response_body['Data'] = data

    json_response = json.dumps(response_body)
    print("Response body:\n" + json_response)

    headers = {
        'content-type': '',
        'content-length': str(len(json_response))
    }

    try:
        response = requests.put(response_url, data=json_response, headers=headers)
        print("Status code: " + response.reason)
    except Exception as e:
        print("send(..) failed executing requests.put(..): " + str(e))

"""
------------------------------------------------------------------------------------------------------------------------
SageMaker lambdas
------------------------------------------------------------------------------------------------------------------------
"""

sm_client = boto3.client('sagemaker')


def prepare_proper_content_format(text):
    return base64.b64encode(text.encode('ascii')).decode('ascii')


def make_endpoint_name(event):
    notebook_instance_name = event['ResourceProperties']['NotebookInstanceName']
    return f'{notebook_instance_name}-endpoint'


def make_model_name(event):
    notebook_instance_name = event['ResourceProperties']['NotebookInstanceName']
    return f'{notebook_instance_name}-model'


def make_lifecycle_config_name(instance_name):
    return f'{instance_name}-lifecycle-configuration'


def create_lifecycle_config(instance_name, event, region):
    config_name = make_lifecycle_config_name(instance_name)
    input_dict = {
        'NotebookInstanceLifecycleConfigName': config_name,
        'OnStart': [{'Content': prepare_proper_content_format('echo "Starting notebook";')}],
    }
    response = sm_client.create_notebook_instance_lifecycle_config(**input_dict)
    print(response)
    if response['ResponseMetadata']['HTTPStatusCode'] == 200:
        return {
            'config_name': config_name,
        }
    else:
        raise Exception


def delete_notebook_instance(instance_name):
    def _exists_with_status(instance_name, status):
        notebooks_with_status = sm_client.list_notebook_instances(
            NameContains=instance_name,
            StatusEquals=status
        )
        return instance_name in [
            ntbk['NotebookInstanceName'] for ntbk in notebooks_with_status['NotebookInstances']
        ]

    if _exists_with_status(instance_name, 'Pending'):
        waiter = sm_client.get_waiter('notebook_instance_in_service')
        waiter.wait(NotebookInstanceName=instance_name)

    if _exists_with_status(instance_name, 'InService'):
        sm_client.stop_notebook_instance(
            NotebookInstanceName=instance_name
        )
        waiter = sm_client.get_waiter('notebook_instance_stopped')
        waiter.wait(NotebookInstanceName=instance_name)

    if _exists_with_status(instance_name, 'Stopped'):
        sm_client.delete_notebook_instance(NotebookInstanceName=instance_name)
    else:
        print(f'Warning! Cannot delete {instance_name}: not found in none of the states: pending, in service or stopped.')


def delete_model(model_name):
    def model_exists():
        models = sm_client.list_models(NameContains=model_name)
        return model_name in [mdl['ModelName'] for mdl in models['Models']]

    if model_exists():
        sm_client.delete_model(ModelName=model_name)
    else:
        print(f'Warning! {model_name} model does not exist')


def delete_endpoint(endpoint_name):
    def endpoint_exists():
        endpoints = sm_client.list_endpoints(NameContains=endpoint_name)
        return endpoint_name in [endpt['EndpointName'] for endpt in endpoints['Endpoints']]

    if endpoint_exists():
        sm_client.delete_endpoint(EndpointName=endpoint_name)
    else:
        print(f'Warning! {endpoint_name} endpoint does not exist')

def delete_lifecycle_config(config_name):
    def lifecycle_config():
        def name(obj):
            return obj['NotebookInstanceLifecycleConfigName']
        lifecycle_configs = sm_client.list_notebook_instance_lifecycle_configs(NameContains=config_name)
        configs = lifecycle_configs['NotebookInstanceLifecycleConfigs']
        return config_name in [name(cnf) for cnf in configs]

    if lifecycle_config():
        sm_client.delete_notebook_instance_lifecycle_config(
            NotebookInstanceLifecycleConfigName=config_name
        )
    else:
        print(f'Warning! {config_name} lifecycle config does not exist')

def lambda_handler(event, context):
    if event['RequestType'] == 'Delete':
        try:
            print('Started deleting SageMaker...')
            print(str(event))
            instance_name = event['ResourceProperties']['NotebookInstanceName']

            delete_notebook_instance(instance_name)
            delete_model(make_model_name(event))
            delete_endpoint(make_endpoint_name(event))
            delete_lifecycle_config(make_lifecycle_config_name(instance_name))

            send_cfnresponse(event, context, CFN_SUCCESS, {})
        except Exception as inst:
            print(inst)
            send_cfnresponse(event, context, CFN_FAILED, {})
    elif event['RequestType'] == 'Create':
        try:
            region = os.environ['AWS_REGION']
            instance_name = event['ResourceProperties']['NotebookInstanceName']
            input_dict = {
                'NotebookInstanceName': event['ResourceProperties']['NotebookInstanceName'],
                'InstanceType': event['ResourceProperties']['NotebookInstanceType'],
                'RoleArn': event['ResourceProperties']['SageMakerRoleArn'],
            }

            config_with_data = create_lifecycle_config(input_dict['NotebookInstanceName'], event,
                                    region)
            input_dict['LifecycleConfigName'] = config_with_data['config_name']
            instance = sm_client.create_notebook_instance(**input_dict)

            waiter = sm_client.get_waiter('notebook_instance_in_service')
            waiter.wait(NotebookInstanceName=event['ResourceProperties']['NotebookInstanceName'])
            print('Sagemager CLI response for creating instance')
            print(str(instance))
            response_data = {
                'SageMakerNotebookURL': f'https://{instance_name}.notebook.{region}.sagemaker.aws/tree?',
            }
            send_cfnresponse(event, context, CFN_SUCCESS, response_data)
        except Exception as ex:
            print('Error!')
            print(ex)
            send_cfnresponse(event, context, CFN_FAILED, {})
