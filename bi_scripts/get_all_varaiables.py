import boto3
import json
import base64
import urllib.parse
import urllib.request
import datetime


def create_airflow_token(env_name):
    mwaa = boto3.client('mwaa')
    mwaa_cli_token = mwaa.create_cli_token(Name=env_name)
    mwaa_auth_token = 'Bearer ' + mwaa_cli_token['CliToken']
    mwaa_webserver_hostname = 'https://{0}/aws_mwaa/cli'.format(mwaa_cli_token['WebServerHostname'])
    return mwaa_auth_token, mwaa_webserver_hostname


def get_from_airflow_api(command, mwaa_auth_token, mwaa_webserver_hostname):
    print(command)
    headers = {
        'Authorization': mwaa_auth_token,
        'Content-Type': 'text/plain'
    }

    data = command.encode('ascii')  # data should be bytes
    req = urllib.request.Request(mwaa_webserver_hostname, data, headers)
    with urllib.request.urlopen(req) as response:
        the_page = response.read()

    json_data = json.loads(the_page)

    mwaa_std_err_message = base64.b64decode(json_data['stderr']).decode('utf8')
    mwaa_std_out_message = base64.b64decode(json_data['stdout']).decode('utf8')

    if mwaa_std_err_message is not None and len(mwaa_std_err_message) > 0 and not mwaa_std_err_message.find(
            "DeprecationWarning"):
        return {
            'statusCode': 400,
            'body': json.dumps(mwaa_std_err_message)
        }
    else:
        return mwaa_std_out_message


# create token for Airflow
mwaa_auth_token, mwaa_webserver_hostname = create_airflow_token('airflow-mobile-test8')

start_time = datetime.datetime.utcnow()
print(start_time)
# get all_vairables
all_vairables = get_from_airflow_api('variables list', mwaa_auth_token, mwaa_webserver_hostname)

all_data = {}
for var in all_vairables.split('\n')[2:]:
    var = var.strip()
    if len(var) <= 1:
        continue
    dicts = {}
    results = get_from_airflow_api(f'variables get {var}', mwaa_auth_token, mwaa_webserver_hostname)
    try:
        data = json.loads(results)
    except:
        data = results.strip()
    all_data[var] = data

with open('../variables.json', 'w') as outfile:
    json.dump(all_data, outfile, indent=4)

end_time = datetime.datetime.utcnow()
print(end_time)
print(f"it took {(end_time - start_time).total_seconds()} seconds")
