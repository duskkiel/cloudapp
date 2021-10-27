import boto3
import json
from boto3 import client
from time import sleep

s3 = boto3.resource('s3')
k = 2

def create():

    for j in keyList:
        try:
            content_object = s3.Object('usu-cs5260-peytonkiel-requests', str(j))
            file_content = content_object.get()['Body'].read().decode('utf-8')
            json_content = json.loads(file_content)
            oname = json_content['owner'].lower().replace(" ", "-")
            wid = json_content['widgetId']

            keyName = "widgets/" + oname + "/" + wid
            
            copy_source = {
            'Bucket': 'usu-cs5260-peytonkiel-requests',
            'Key': str(j)
            }

            s3.meta.client.copy(copy_source, 'usu-cs5260-peytonkiel-web', keyName)
            print("Created Object in Bucket: usu-cs5260-peytonkiel-web (Key: " + keyName + ")")

            s3.Object('usu-cs5260-peytonkiel-requests', str(j)).delete()
            

        except KeyError:
            print('ERROR: CREATION OF OBJECT CONSUMED AT KEY ' + j + " FAILED")
            s3.Object('usu-cs5260-peytonkiel-requests', str(j)).delete()

    keyList.clear()



while k == 2:
    try:
        keyList = []
        conn = client('s3')
        for key in conn.list_objects(Bucket='usu-cs5260-peytonkiel-requests')['Contents']:
            keyList.append(key['Key'])

        keyList.sort(key = int)

        create()

        
    except KeyError:
        print("Empty Bucket")
        sleep(1)