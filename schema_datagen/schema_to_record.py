import json
import sys
import os
import requests
import getpass
import random
import time
import datetime
import argparse

'''
this script takes in a push json schema and streams records based on its schema to a saas streaming endpoint. It should be run in python 3.6

Usage:
python schema_to_record.py --user greg.throne@molecula.com --schema alltypes.schema.json --values alltypes.valuegen.json --source <sink id>

Assumptions: path column is name to use. json is not nested. field key is an ID
-- values options:
decimal: (min,max)
int: (min,max)
string: [<list> of words]
strings: [<list> of words]
timestamp: ("min date","max date") #Note this is only configured to load at seconds granualarity currently
bool: [True,False]

'''

def read_schema(schema):
    """ Read in a push/sink data schema to get schema to generate for

    Args:
        schema (path): relative or absolute path to schema file

    Returns:
        json: schema
    """

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
    """read in dictionary file with values to generate for fields

    Args:
        value_dict (path): relative or absolute path to schema file

    Returns:
        dict: dictionary of values for fields
    """

    f = open(value_dict)
    data = f.read()
    values =eval(data)
    f.close()
    return values

def random_date(start, end):
    """Generate a random datetime between `start` and `end`

    Args:
        start (date): start date
        end (date): end date

    Returns:
       date: date between start and end in the featurebase date format (seconds)
    """
    ts = start + datetime.timedelta(
        # Get a random amount of seconds between `start` and `end`
        seconds=random.randint(0, int((end - start).total_seconds())),
    )
    return ts.strftime("%Y-%m-%dT%H:%M:%S%Z") + "Z"

def create_record(data, idkey, values=None):
    """generate an individual record based inputed data structures, id number, and optional values to choose from

    Args:
        data (json): schema to match
        idkey (int): integer used as the key for featurebase
        values (dict, optional): dictionary of values for fields. Defaults to None.

    Returns:
        json: individual record to batch and post
    """
    record={}
    for col in data['schema']['definition']:
        # for each field check type and if there are values that should be randomly chose from
        if col['name'] == data['schema']['id_field']:
            record[col['path'][0]] = idkey
            continue
        elif col['type'] in ['id','int']:
            if values and values[col['name']]:
                record[col['path'][0]] = random.randint(values[col['name']][0], values[col['name']][1])
            else:
                record[col['path'][0]] = random.randint(0,100)
        elif col['type'] == 'string':
            if values and values[col['name']]:
                record[col['path'][0]] = random.choice(values[col['name']])
            else:
                record[col['path'][0]] ="string"
        elif col['type'] == 'decimal':
            if values and values[col['name']]:
                record[col['path'][0]] = str(round(random.uniform(values[col['name']][0], values[col['name']][1]), 1))
            else:
                record[col['path'][0]] = str(round(random.uniform(0.0, 100.0), 1))
        elif col['type'] == 'timestamp':
            if values and values[col['name']]:
                record[col['path'][0]] = str(random_date(datetime.datetime.strptime(values[col['name']][0],"%Y-%m-%d"),datetime.datetime.strptime(values[col['name']][1],"%Y-%m-%d")))
            else:
                record[col['path'][0]] ="2006-01-02T15:04:05Z"
        elif col['type'] == 'strings':
            if values and values[col['name']]:
                record[col['path'][0]] = random.choices(values[col['name']],k=random.randint(0,len(values[col['name']])))
            else:
                record[col['path'][0]] = ["a","b"]
        elif col['type'] == 'ids':
            if values and values[col['name']]:
                record[col['path'][0]] = random.choices(values[col['name']],k=random.randint(0,len(values[col['name']])))
            else:
                record[col['path'][0]] = [1,2]
        elif col['type'] == 'bool':
            if values and values[col['name']]:
                record[col['path'][0]] = random.choice(values[col['name']])
            else:
                record[col['path'][0]] = True
        else:
            record[col['path'][0]] = "diffdatatype"
    return record



def json_payload(records):
    """Create a json file with n records in the format needed by streaming endpoint. writes a file with current batch to troublehoot but also returns the records

    Args:
        records (json): 1 to n json records

    Returns:
        (json): 1 to n json records batched together
    """
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
    """get auth token for featurebase sas

    Args:
        user (string): username
        pw (string): password

    Returns:
        string: idtoken for auth
    """
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
    """ take in 1:n records and post them to featurebase saas via a streaming endpoint (sink)

    Args:
        token string: idtoken for auth
        sink_id (string): sink id to push to
        records (json): 1 to n json records
        datahost (_type_): molecula dataplane host to push to

    Returns:
        int: count of successfull records if no errors
    """
    body = json.dumps(
        json.loads(records)
    )
    headers = {"Content-Type": "tapplication/json",
        "Authorization": f'Bearer {token}'}
    print('Posting Records')
    post = requests.post(datahost+'/sinks/'+sink_id, headers=headers,data=body).json()

    if post['error_count'] > 0:
        print(f'There were {post["error_count"]} failed records. System Existing')
        print(post)
        exit()
    else:
        return post['success_count']


def main():
    """main function to run. expects three args
    Args:
        --user: user name
       --schema: relative or absolute path to schema file
        --values : relative or absolute path to values dict
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--user', type=str, required=True)
    parser.add_argument('--schema', type=str, required=True)
    parser.add_argument('--source', type=str, required=True)
    parser.add_argument('--values', type=str, required=False)
    args = parser.parse_args()
    data = read_schema(args.schema)
    if args.values:
        values = read_values(args.values)
        print(f'The following field values to use when generating records: {values}')
    else:
        values = None
    user =  args.user

    try:
        pw = getpass.getpass()
    except Exception as error:
        print('ERROR', error)
    else:
        print('Password entered')
    token = auth(user,pw)
    sink_id = args.source
    datahost = 'https://data.molecula.cloud/v1'

    # record count is total records you want to end with
    record_cnt = 4000
    # idkey is the starting id number that will be incremented up
    idkey = 1
    # batch size is the amount of records sent per request. Saas can only handle 1k max currentlyh
    batchsize = 1000
    # Leave as zero. This increments up to the record count
    current_cnt = 0
    # create batches of records equal to batch size (unless about to reach target) and send. This sleeps for 1 second between batches
    while current_cnt < record_cnt:
        if current_cnt + batchsize > record_cnt:
            last_cnt = record_cnt - current_cnt
            records = [create_record(data,idkey + i,values) for i in range(0,last_cnt)]
            pushrecords = json_payload(records)
            print(pushrecords)
            success = post_records(token, sink_id,pushrecords,datahost)
            print(f'{success} records ingested')
            print(records)
            current_cnt = record_cnt
        else:
            records = [create_record(data,idkey + i,values) for i in range(0,batchsize)]
            pushrecords = json_payload(records)
            success = (post_records(token, sink_id,pushrecords,datahost))
            print(f'{success} records ingested')
            current_cnt += batchsize
            idkey += batchsize
            time.sleep(1)
    print(f"All done! {current_cnt} records ingested!")



if __name__ == "__main__":
    main()
