import os, json, ConfigParser,sys, ndjson, datetime , gzip
from functions import *

CLOUDFLARE_BASE_DIR=os.environ["CLOUDFLARE_BASE_DIR"]

EVENT_TYPE = '_'.join(os.path.basename(__file__).split('.')[0].split('_')[1:])

config = ConfigParser.ConfigParser()
config.read(CLOUDFLARE_BASE_DIR + "/config/conf.ini")

BUCKET = config.get(EVENT_TYPE, 'BUCKET')
JSON_KEY = config.get(EVENT_TYPE, 'JSON_KEY')
TEST_FILE = config.get(EVENT_TYPE, 'TEST_FILE')
LAST_EVENT_FILE = config.get(EVENT_TYPE, 'LAST_EVENT_FILE')

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=CLOUDFLARE_BASE_DIR+'/'+JSON_KEY

# 1 Find latest file

#TESTING
test_file = gzip.open(CLOUDFLARE_BASE_DIR+'/'+TEST_FILE)

event_list = ndjson.load(test_file)

last_event_datetime = datetime_from_file(LAST_EVENT_FILE) 



# 2 Find new events
filtered_event_list = []
max_datetime = last_event_datetime
for event in event_list:
    EdgeEndTimestamp=event['EdgeEndTimestamp']
    event_datetime = datetime.datetime.strptime(EdgeEndTimestamp, '%Y-%m-%dT%H:%M:%SZ')
    if event_datetime > last_event_datetime:
        filtered_event_list.append(event)
        max_datetime = max(max_datetime,event_datetime)

# No new data
if len(filtered_event_list) == 0:
    print("No new events")
    exit(0)

# Send unique new data to QRadar via HTTP ( One large POST or multiple small POSTS ? )
for event in filtered_event_list:
    print(event['EdgeEndTimestamp'])


# Update the last event datetime to the max event datetime seen on new events
write_last_event(LAST_EVENT_FILE, str(max_datetime))


#        Uncomment to download a specific file by its path locally as test_data.json
# local_path ='./' + server_json_file.split('/')[-1]
# download_blob(bucket_name, server_json_file , local_path )

# blob_metadata(bucket_name, server_json_file)
# server_json_file_date = blob_modified_date(bucket_name, server_json_file)
# server_test_folder_date = blob_modified_date(bucket_name, servet_test_folder)