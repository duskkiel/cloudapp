import boto3
import json
from boto3 import client
from time import sleep
import sys

s3 = boto3.resource('s3')
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table = dynamodb.Table("widgets")
k = 2
Bucket = 'usu-cs5260-peytonkiel-requests'

# secondCom = sys.argv[1]
# thirdCom = sys.argv[2]


def dynamo():
    for j in keyList:
        try:
            content_object = s3.Object(Bucket, str(j))
            file_content = content_object.get()['Body'].read().decode('utf-8')
            json_content = json.loads(file_content)
            oname = json_content['owner'].lower().replace(" ", "-")
            wid = json_content['widgetId']
            ty = json_content['type']
            rid = json_content['requestId']
            lab = json_content['label']
            des = json_content['description']
            oa = json_content['otherAttributes']

            table.put_item(
                TableName='widgets',
                Item = {
                    "id": str(j),
                    "type":str(ty),
                    "requestId":str(rid),
                    "widgetId":str(wid),
                    "owner":str(oname),
                    "label":str(lab),
                    "description":str(des),
                    "otherAttributes":str(oa)
                }
                    )


            s3.Object(Bucket, str(j)).delete()
            

        except KeyError:
            print('ERROR: CREATION OF OBJECT CONSUMED AT KEY ' + j + " FAILED")
            s3.Object(Bucket, str(j)).delete()

    keyList.clear()


def create():

    for j in keyList:
        try:
            content_object = s3.Object(Bucket, str(j))
            file_content = content_object.get()['Body'].read().decode('utf-8')
            json_content = json.loads(file_content)
            oname = json_content['owner'].lower().replace(" ", "-")
            wid = json_content['widgetId']

            keyName = "widgets/" + oname + "/" + wid
            
            copy_source = {
            'Bucket': Bucket,
            'Key': str(j)
            }

            s3.meta.client.copy(copy_source, 'usu-cs5260-peytonkiel-web', keyName)
            print("Created Object in Bucket: usu-cs5260-peytonkiel-web (Key: " + keyName + ")")

            s3.Object(Bucket, str(j)).delete()
            

        except KeyError:
            print('ERROR: CREATION OF OBJECT CONSUMED AT KEY ' + j + " FAILED")
            s3.Object(Bucket, str(j)).delete()

    keyList.clear()



while k == 2:
    try:
        keyList = []
        conn = client('s3')
        for key in conn.list_objects(Bucket=Bucket)['Contents']:
            keyList.append(key['Key'])

        keyList.sort(key = int)

        # create()
        dynamo()

    except KeyError:
        print("Empty Bucket")
        sleep(1)