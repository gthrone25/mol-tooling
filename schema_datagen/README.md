#Overview

This script takes in a push json schema and streams records based on its schema to a saas streaming endpoint. It should be run in python 3.6.
You must have a sink running with the same schema as found in this repo to use this example, but it should work with any schema. 

## Usage:

` python schema_to_record.py --user <saas user name> --schema alltypes.schema.json --values alltypes.valuegen.json --source <sink id> `

## Variables to change:

record count is total records you want to end with
record_cnt = 4000

idkey is the starting id number that will be incremented up
idkey = 1

batch size is the amount of records sent per request. Saas can only handle 1k max currentlyh
batchsize = 1000

Leave as zero. This increments up to the record count
current_cnt = 0

