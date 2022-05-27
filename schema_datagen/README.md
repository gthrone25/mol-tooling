This script takes in a push json schema and streams records based on its schema to a saas streaming endpoint. It should be run in python 3.6.
You must have a sink running with the same schema as found in this repo to use this example, but it should work with any schema. 

Usage:

` python schema_to_record.py --user <saas user name> --schema alltypes.schema.json --values alltypes.valuegen.json --source <sink id> `
