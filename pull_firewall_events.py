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


def list_blobs(bucket_name,start_offset=None,max_results=None):
    storage_client = storage.Client()
    blobs = storage_client.list_blobs(bucket_name,start_offset=start_offset, max_results=int(max_results) if max_results is not None else None)
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


def upload_remote_bucket_files(last_file_imported,max_results=None):
    all_blobs=list_blobs(BUCKET,last_file_imported, max_results)
    for blob in all_blobs:
        if is_file(blob.name) and not blob.name == last_file_imported :             
        
            local_filename = blob.name.split("/")[-1].split(".")[0] + ".ndjson"

            download_blob(BUCKET,blob.name, local_filename)
            file_has_failed = parse_blob_and_post(local_filename)
            if not file_has_failed:
                with open("remember_filename.txt","w") as local_file:
                    local_file.write(blob.name)

def last_file_imported():
    with open("remember_filename.txt","r") as local_file:
        return local_file.readline()

def parse_blob_and_post(local_filename):
    with open(local_filename,"rw") as local_file:
        # Try to parse the ndjson file
        event_list = ndjson.load(local_file)          
        file_has_failed = False      
        while len(event_list) != 0:
            event = event_list.pop()
            resp = None
            try: 
               resp = requests.post(QRADAR_URL, json=event, timeout=5 )
               if resp.status_code != 200:
                    print("File has failed to post")
                    file_has_failed = True
                    break
            except Exception as e:
                print("Exception! File has failed to post")
                file_has_failed = True
                break
            

        print("File ingested by QRadar")
       
    os.remove(local_filename)
    return file_has_failed




if  __name__=="__main__":
    if len(sys.argv) != 2 : max_results = None 
    else: max_results = sys.argv[1]
    last_file_imported = last_file_imported()
    upload_remote_bucket_files(last_file_imported,max_results)

