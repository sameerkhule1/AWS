import json
import urllib.parse
import boto3

print('Loading function')

s3 = boto3.client('s3')


def lambda_handler(event, context):
    #print("Received event: " + json.dumps(event, indent=2))

    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    print("bucket --->", bucket)
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        fileContent = response["Body"].read()
        listOfContent = fileContent.split()
        wordsCont = len(listOfContent)

        notification = f"The word count in the file {key} is {wordsCont}."
        client = boto3.client('sns')
        response = client.publish (
            TargetArn = "arn:aws:sns:us-west-2:379281256766:count-words-topic",
            Message = json.dumps({'default': notification}),
            MessageStructure = 'json'
            )
            
        return {
            'statusCode': 200,
            'body': json.dumps(response)
        }
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e