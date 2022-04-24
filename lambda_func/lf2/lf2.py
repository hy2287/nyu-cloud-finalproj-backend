import boto3
import json
import time

athena = boto3.client('athena')
quicksight = boto3.client('quicksight')

dynamo = boto3.client('dynamodb')

def getQuickSightUrl():
    response = quicksight.generate_embed_url_for_registered_user(
        AwsAccountId='617440612116',
        SessionLifetimeInMinutes=600,
        UserArn='arn:aws:quicksight:us-east-1:617440612116:user/default/617440612116',
        ExperienceConfiguration={
            'Dashboard': {'InitialDashboardId': '6ccb3230-44ce-4c47-af10-ae80c12bd1fc'}
        }
    )
    return response
        

def get_var_char_values(d):
    return [obj['VarCharValue'] for obj in d['Data']]
    
def respond(err, res=None):
    return {
        'statusCode': '400' if err else '200',
        'body': err.message if err else json.dumps(res),
        'headers': {
            'Content-Type': 'application/json',
            "Access-Control-Allow-Origin": "*",
        },
    }

def getTop3Players(hours, number=3, sentiment=None):
    queryString = 'SELECT "player_full_name", COUNT(*) FROM "AwsDataCatalog"."test"."alltweets" WHERE "created_at" > NOW() - INTERVAL '
    queryString += "'"+str(hours)+"'" + ' HOUR'
        
    if sentiment=="POSITIVE" or sentiment=="NEGATIVE":
        queryString +=  ' AND "sentiment"=' + "'"+sentiment+"'"
    
    queryString += ' GROUP BY "player_full_name" ORDER BY COUNT(*) DESC LIMIT ' + str(number)
    s3location, athenaResult = queryAthena(queryString)
    return athenaResult
    
def queryAthena(queryString):
    queryStart = athena.start_query_execution(
        QueryString = queryString,
        QueryExecutionContext = {
            'Database': 'test'
        }, 
        ResultConfiguration = { 'OutputLocation': 's3://aws-athena-query-results-us-east-1-617440612116'}
    )
    queryExecutionID = queryStart['QueryExecutionId']
    
    while True:
        response_get_query_details = athena.get_query_execution(QueryExecutionId=queryExecutionID)
        status = response_get_query_details['QueryExecution']['Status']['State']
        if (status == 'FAILED') or (status == 'CANCELLED') :
            failure_reason = response_get_query_details['QueryExecution']['Status']['StateChangeReason']
            print(failure_reason)
            return False, False
        elif status == 'SUCCEEDED':
            location = response_get_query_details['QueryExecution']['ResultConfiguration']['OutputLocation']

            ## Function to get output results
            response_query_result = athena.get_query_results(QueryExecutionId = queryExecutionID)
            result_data = response_query_result['ResultSet']
            
            if len(response_query_result['ResultSet']['Rows']) > 1:
                header = response_query_result['ResultSet']['Rows'][0]
                rows = response_query_result['ResultSet']['Rows'][1:]
                header = [obj['VarCharValue'] for obj in header['Data']]
                result = [dict(zip(header, get_var_char_values(row))) for row in rows]
                return location, result
            else:
                return location, None
        else:
            time.sleep(5)
    
def lambda_handler(event, context):
    '''Demonstrates a simple HTTP endpoint using API Gateway. You have full
    access to the request and response payload, including headers and
    status code.

    To scan a DynamoDB table, make a GET request with the TableName as a
    query string parameter. To put, update, or delete an item, make a POST,
    PUT, or DELETE request respectively, passing in the payload to the
    DynamoDB API as a JSON body.
    '''
    print(event)
    quicksightRes = getQuickSightUrl()
    athenaRes = getTop3Players(2400, 10, "NEGATIVE")
    
    respondBody = {
        'athenaQueryRes': athenaRes,
        'quicksightRes': quicksightRes
    }
    return respond(None, respondBody)

    # operations = {
    #     'DELETE': lambda dynamo, x: dynamo.delete_item(**x),
    #     'GET': lambda dynamo, x: b,
    #     'POST': lambda dynamo, x: dynamo.put_item(**x),
    #     'PUT': lambda dynamo, x: dynamo.update_item(**x),
    # }

    # operation = event['httpMethod']
    # if operation in operations:
    #     payload = event['queryStringParameters'] if operation == 'GET' else json.loads(event['body'])
    #     return respond(None, operations[operation](dynamo, payload))
    # else:
    #     return respond(ValueError('Unsupported method "{}"'.format(operation)))
