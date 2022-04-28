import json
import urllib3
import boto3

sns = boto3.client('sns')
http = urllib3.PoolManager()

def dispatchTopics(topics):
    for topic in topics:
        dispatchTopic(topic['TopicArn'])

def dispatchTopic(topicArn):
    name = getPlayerName(topicArn)
    print(name)
    url='https://sgc0m5do03.execute-api.us-east-1.amazonaws.com/dev/playerv2'
    params={"fullname":name}
    response = http.request('GET',url,fields=params)
    res_json = json.loads(response.data)
    
    #TO DO:
    message = json.dumps(res_json)
    
    sns_response = sns.publish(
        TopicArn=topicArn,
        Message=message,
        Subject=name+' Daily Update',
        MessageStructure='string'
    )
    print(sns_response)
    
def getPlayerName(topicArn):
    words = topicArn.split(':')
    name = words[-1].replace('_',' ')
    return name

def lambda_handler(event, context):
    list_topic_res = sns.list_topics()
    topics = list_topic_res['Topics']
    dispatchTopics(topics)
    nextToken = list_topic_res['NextToken'] if 'NextToken' in list_topic_res else None
    while nextToken is not None:
        list_topic_res = sns.list_topics(nextToken)
        topics = list_topic_res['Topics']
        dispatchTopics(topics)
        nextToken = list_topic_res['NextToken'] if 'NextToken' in list_topic_res else None
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
