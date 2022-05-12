import json
import sys
import os
import requests
import getpass
import random
import time
import datetime
import argparse

#this script takes in a push json schema and streams records based on its schema to a saas streaming endpoint
# assumptions: path column is name to use. json is not nested


def read_schema(schema):

    f = open(schema)
    data = json.load(f)
    f.close()

    try:
        data['schema']['id_field']
    except KeyError as error:
        print('Only schemas wiht id fields can be used with this script', error)
        exit()
    else:
        return data

def read_values(value_dict):

    f = open(value_dict)
    data = f.read()
    values =eval(data)
    f.close()
    return values

def random_date(start, end):
    """Generate a random datetime between `start` and `end`"""
    ts = start + datetime.timedelta(
        # Get a random amount of seconds between `start` and `end`
        seconds=random.randint(0, int((end - start).total_seconds())),
    )
    return ts.strftime("%Y-%m-%dT%H:%M:%S%Z") + "Z"

def create_record(data, idkey, values=None):
    record={}
    for col in data['schema']['definition']:
        if col['name'] == data['schema']['id_field']:
            record[col['path'][0]] = idkey
            continue
        elif col['type'] in ['id','int']:
            if values and values[col['name']]:
                record[col['path'][0]] = random.randint(values[col['name']][0], values[col['name']][1])
            else:
                record[col['path'][0]] = random.randint(0,100)
        elif col['type'] in ['string']:
            if values and values[col['name']]:
                record[col['path'][0]] = random.choice(values[col['name']])
            else:
                record[col['path'][0]] ="string"
        elif col['type'] in ['decimal']:
            if values and values[col['name']]:
                record[col['path'][0]] = str(round(random.uniform(values[col['name']][0], values[col['name']][1]), 1))
            else:
                record[col['path'][0]] = str(round(random.uniform(0.0, 100.0), 1))
        elif col['type'] in ['timestamp']:
            if values and values[col['name']]:
                record[col['path'][0]] = str(random_date(datetime.datetime.strptime(values[col['name']][0],"%Y-%m-%d"),datetime.datetime.strptime(values[col['name']][1],"%Y-%m-%d")))
            else:
                record[col['path'][0]] ="2006-01-02T15:04:05Z07:00"
        elif col['type'] in ['strings']:
            if values and values[col['name']]:
                record[col['path'][0]] = random.choices(values[col['name']],k=random.randint(0,len(values[col['name']])))
            else:
                record[col['path'][0]] = '["a","b"]'
        elif col['type'] in ['ids']:
            if values and values[col['name']]:
                record[col['path'][0]] = random.choices(values[col['name']],k=random.randint(0,len(values[col['name']])))
            else:
                record[col['path'][0]] = "[1,2]"
        elif col['type'] in ['bool']:
            if values and values[col['name']]:
                record[col['path'][0]] = random.choice(values[col['name']])
            else:
                record[col['path'][0]] ="string"
        else:
            record[col['path'][0]] = 'diffdatatype'
    return record



def json_payload(records):
    with open('json_data.json','w') as recfile:
        for item in records:
            recfile.write(f'{{ "value":{json.dumps(item)}}},\n')
        recfile.seek(0, 2)              # seek to end of file; f.seek(0, os.SEEK_END) is legal. 2 is seek to files end
        recfile.seek(recfile.tell() - 2, 0)  # seek to the second last char of file; f.seek(f.tell()-2, os.SEEK_SET) is legal. 0 is absolute file postioining
        recfile.truncate()

    f = open('json_data.json')
    data = f.read()
    f.close()
    pushrecords = f'''{{
        "records": [
        {data}
        ]
        }}'''
    return pushrecords


def auth(user,pw):
    body = json.dumps({
        "USERNAME": str(user),
        "PASSWORD": str(pw)
    })
    auth_headers = {"Content-Type": "application/x-amz-json-1.1",
            "X-Amz-Target": "AWSCognitoIdentityProviderService.InitiateAuth"
            }
    token = requests.post('https://id.molecula.cloud',data=body).json()
    idtoken = token['AuthenticationResult']['IdToken']
    return idtoken




def post_records(token, sink_id, records,datahost):
    body = json.dumps(
        json.loads(records)
    )
    headers = {"Content-Type": "text/plain",
        "Authorization": token}
    print('Posting Records')
    post = requests.post(datahost+'/sinks/'+sink_id, headers=headers,data=body).json()
    #{'code': 'PayloadTooLarge', 'message': 'Request exceeded record count limit'}
    if post['error_count'] > 0:
        print(f'There were {post["error_count"]} failed records. System Existing')
        print(post)
        exit()
    else:
        return post['success_count']


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--user', type=str, required=True)
    parser.add_argument('--schema', type=str, required=True)
    parser.add_argument('--values', type=str, required=False)
    args = parser.parse_args()
    data = read_schema(args.schema)
    if args.values:
        values = read_values(args.values)
        print(values)
    else:
        values = None
    user =  args.user #'greg.throne@molecula.com'

    try:
        pw = getpass.getpass()
    except Exception as error:
        print('ERROR', error)
    else:
        print('Password entered')
    token = auth(user,pw)
    sink_id = 'f58630ca-5214-4eed-95dc-fdc72d28ece7'
    datahost = 'https://data.molecula.cloud/v1'

    record_cnt = 500000
    idkey = 1
    batchsize = 1000
    current_cnt = 0
    while current_cnt < record_cnt:
        if current_cnt + batchsize > record_cnt:
            last_cnt = record_cnt - current_cnt
            records = [create_record(data,idkey + i,values) for i in range(0,last_cnt)]
            pushrecords = json_payload(records)
            success = post_records(token, sink_id,pushrecords,datahost)
            print(f'{success} records ingested')
            print(records)
            current_cnt = record_cnt
        else:
            records = [create_record(data,idkey + i) for i in range(0,batchsize)]
            pushrecords = json_payload(records)
            success = (post_records(token, sink_id,pushrecords,datahost))
            print(f'{success} records ingested')
            current_cnt += batchsize
            idkey += batchsize
            time.sleep(1)
    print(f"All done! {current_cnt} records ingested!")






if __name__ == "__main__":
    main()


    # now = datetime.datetime.now()
    # print(now.strftime("%Y-%m-%dT%H:%M:%S%Z") + "Z")
    # reg_format_date = datetime.datetime.utcnow().isoformat() + "Z"
    # print(reg_format_date)
