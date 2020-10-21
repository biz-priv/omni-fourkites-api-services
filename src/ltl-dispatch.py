import psycopg2
import logging
import json
import datetime
from datetime import datetime,timezone
import boto3
import requests
from requests.auth import HTTPBasicAuth
logger = logging.getLogger()
logger.setLevel(logging.INFO)
client = boto3.client('dynamodb')
from boto3.dynamodb.conditions import Key, Attr

from src.common import modify_date
from src.common import validateDynamoDB
from src.common import execute_db_query

def handler(event, context):
    try :
        query = 'Select shipper_name,reference_nbr,trim(x12_status),x12_status_desc,event_date,pod,pod_date,status,file_nbr from fourkites_ltl WHERE message_sent = '''
        queryData = execute_db_query(query)
        for results in queryData:
            temp = recordsConv(results,con,cur)
            if temp != 'sucess':
                records_list.append(temp)

        shipment_records = {'updates':records_list}
        payload = json.dumps(shipment_records)
        logger.info("Payload loaded into Fourkites API  :{}".format(payload))

        headers = {'content-type': 'application/json'}
        r = requests.post(url, headers=headers,data=payload,auth=HTTPBasicAuth(os.environ['fourkites_username'],os.environ['fourkites_password']))
        logger.info("Response from fourkites API :{}".format(r))
    except Exception as e:
        logging.exception("ApiPostError: {}".format(e))
        raise ApiPostError(json.dumps({"httpStatus": 400, "message": "Api post error."}))

def recordsConv(y,con,cur):
    try:
        record = {}
        record["shipper"] = y[0]
        record["billOfLading"] = y[1]
        record["statusCode"] = y[2]
        record["statusDescription"] = y[3]
        record["eventTimestamp"] = modify_date(y[4])
        record["delivered"] = y[5]
        record["deliveredAt"] = modify_date(y[6])
        status1 = y[7]
        file_nbr = y[8]
        shipper = y[0]
        billoflading = y[1]
        statuscode = y[2]
        status = y[3]
        eventTimestamp = record["eventTimestamp"]
        delivered = y[5]
        deliveredAt = record["deliveredAt"]

        filenumber = file_nbr+billoflading+status1
        print('Order number is:', filenumber)

        if validateDynamoDB(filenumber) == 'success':
            print('Files doesnt exist')
            updateDynamoDB(shipper,billoflading,statuscode,status,eventTimestamp,delivered,deliveredAt,status1,file_nbr)
            cur.execute(f"UPDATE public.fourkites_ltl SET message_sent = 'Y' where file_nbr = '{file_nbr}' and status = '{status1}' and reference_nbr = '{billoflading}'")
            con.commit()
            return record
            cur.close()
            con.close()
        else:
            print('Record exists in DynamoDB')
            return 'success'
    except Exception as e:
        logging.exception("RecordConversionError: {}".format(e))
        raise RecordConversionError(json.dumps({"httpStatus": 400, "message": "Record conversion error."}))

        
def updateDynamoDB(shipper,billoflading,statuscode,status,eventTimestamp,delivered,deliveredAt,status1,file_nbr):
    try:
        x = datetime.now()
        x = dateconv(x)
        response = client.put_item(
            TableName = os.environ['fourkites_tablename'],
            Item={
                'FileNumber': {
    		    'S': file_nbr+billoflading+status1
    		    },
    		    'BillOfLading': {
    		    'S': billoflading
    		    },
    		    'StatusCode':{
    		    'S': statuscode
    		    },
    		    'StatusDescription':{
    		    'S': status
    		    },
    		    'EventTimestamp':{
    		    'S': eventTimestamp
    		    },
    		    'Delivered':{
    		    'S': delivered
    		    },
    		    'DeliveredAt':{
    		    'S': deliveredAt
    		    },
    		    'RecordInsertTime':{
    		    'S': x
    		    }
            }
            )
        return response
    except Exception as e:
        logging.exception("UpdateDynamodbError: {}".format(e))
        raise UpdateDynamodbError(json.dumps({"httpStatus": 400, "message": "DynamoDB Update Error"}))

class RecordConversionError(Exception): pass
class ApiPostError(Exception): pass
class UpdateDynamodbError(Excpetion): pass