#!/usr/bin/python
import os, json, ConfigParser,sys, ndjson, datetime \
, gzip, requests
from threading import local
from google.cloud import storage


CLOUDFLARE_BASE_DIR="/root/pull_cloudflare_events"
LAST_EVENT_TIME_FILE="last_event_time.txt"
config = ConfigParser.ConfigParser()
config.read("./conf.ini")
BUCKET = config.get("cloudflare", 'BUCKET')
JSON_KEY_PATH = config.get("cloudflare", 'JSON_KEY_PATH')
QRADAR_URL=config.get("cloudflare",'QRADAR_URL')
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=JSON_KEY_PATH


def list_blobs(bucket_name,max_results):
    storage_client = storage.Client()
    blobs = storage_client.list_blobs(bucket_name,max_results=int(max_results) if max_results != None else None)
    return [blob for blob in blobs]


def download_blob(bucket_name, source_blob_name, destination_file_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)
    print(
        "Downloaded storage object {} from bucket {} to local file {}.".format(
            source_blob_name, bucket_name, destination_file_name
        )
    )

def is_file(filename): return filename.split('/')[1]!=''

def write_to_json(filename):
   outfile=filename.strip('.log')+'.json'
   outfile=open(outfile,'a')
   infile=open(filename,'r')
   for event in infile.readlines():
       json.dump(event,outfile)
   outfile.close()
   infile.close()

def upload_local_failed_files():
    # First Check for local log files that failed QRadar ingestion
    for failed_file in os.listdir('./'):
        if failed_file.startswith('failed_'):
            parse_blob_and_post(failed_file, True)
   
def upload_remote_bucket_files(max_results):
    all_blobs=list_blobs(BUCKET, max_results)
    file_index = 0
    for blob in all_blobs:
        if is_file(blob.name): 
            local_filename = blob.name.split("/")[-1].split(".")[0] + ".ndjson"
            download_blob(BUCKET,blob.name, local_filename)
            parse_blob_and_post(local_filename, False)
            blob.delete()


def parse_blob_and_post(local_filename, file_retry):
    with open(local_filename,"rw") as local_file:
        
        try:
            # Try to parse the ndjson file
            event_list = ndjson.load(local_file)          
            file_has_failed=False      
            initial_len = len(event_list)
            while len(event_list) != 0:
                print(len(event_list))
                event = event_list.pop()
                
                # Real scenario 
                # resp = requests.post(QRADAR_URL, json=event, timeout=10 )


                # Testing with fail after 4 inserts
                resp = requests.post('https://webhook.site/a2d08685-6d49-4819-b33a-eec11aa127c5', json=event, timeout=10 )
                if len(event_list) == initial_len-3 and not file_retry: resp.status_code = 301
                

                # If failed write to disk
                if resp.status_code != 200:
                    event_list.append(event)
                    file_has_failed=True
                    if file_retry: failed_local_filename = local_filename
                    else: failed_local_filename = "failed_" + local_filename
                    print(failed_local_filename)
                    with open(failed_local_filename, 'w') as failed_file: ndjson.dump(event_list, failed_file) 
                    raise Exception('POST Request failed. Backing up file locally for future uploads')
            print("File ingested by QRadar")
        except Exception as e:
            print(e.message, e.args)

    if not file_has_failed: os.remove(local_filename)
    else:
        if not file_retry: os.remove(local_filename) 


if  __name__=="__main__":
    if len(sys.argv) != 2 : max_results = None 
    else: max_results = sys.argv[1]
    
    upload_local_failed_files()
    

    upload_remote_bucket_files(max_results)

