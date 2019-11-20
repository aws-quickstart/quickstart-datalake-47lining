import boto3
import json


class PublishTopicException(Exception):
    pass


def learn_more(config, form):
    print('SNS form {}'.format(form))
    topic_arn = config['sns_learn_more_topic_arn']
    payload = {
        'name': form['name'],
        'role': form['role'],
        'email': form['email'],
        'company': form['company'],
        'message': form['message']
    }

    region = topic_arn.split(':')[3]
    client = boto3.client('sns', region_name=region)
    response = client.publish(
        TopicArn=topic_arn,
        Message=json.dumps(payload),
        Subject='Data Lake Learn More request from {}'.format(payload['name']))

    print('SNS response {}'.format(response))
    if response['ResponseMetadata']['HTTPStatusCode'] != 200:
        raise PublishTopicException()
