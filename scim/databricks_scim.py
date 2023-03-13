import requests
import argparse

parser = argparse.ArgumentParser(
                    prog='Databricks_SCIM_CLI',
                    description='Retrieve info about SCIM Assets')

parser.add_argument('--host', required=True)   
parser.add_argument('--token', required=True)
parser.add_argument('--resource', choices=['Users', 'ServicePrincipals'], required=True)

args = parser.parse_args()
#print(args.host, args.token)

host = args.host
token = args.token
resource = args.resource

r = requests.get(host + '/api/2.0/preview/scim/v2/' + resource, headers={'Authorization': 'Bearer ' + token})
users_json = r.json()
#print(users_json)

for resource in users_json["Resources"]:
    print(resource["displayName"] + "," + resource["id"])
