#!/usr/bin/python
import os, json, ConfigParser,sys, ndjson, datetime , gzip,requests
from google.cloud import storage

#VARIABLES

CLOUDFLARE_BASE_DIR="/root/pull_cloudflare_events"
LAST_EVENT_TIME_FILE="last_event_time.txt"
config = ConfigParser.ConfigParser()
config.read(CLOUDFLARE_BASE_DIR + "/conf.ini")

BUCKET = config.get("cloudflare", 'BUCKET')
JSON_KEY = config.get("cloudflare", 'JSON_KEY')
QRADAR_URL=config.get("cloudflare",'QRADAR_URL')

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=CLOUDFLARE_BASE_DIR+'/'+JSON_KEY


#FUNCTIONS


def list_blobs(bucket_name):
    storage_client = storage.Client()
    blobs = storage_client.list_blobs(bucket_name)
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

def is_file(filename):
    if filename.split('/')[1]!='':
      return True
    else:
      return False

def send_to_qradar(filename):
    logfile=open(filename,'r')
    events=logfile.readlines()
    logfile.close()
    for event in events:
        requests.post(QRADAR_URL,data=event)

def post_to_qradar(filename):
   files={'files': open(filename,'rb')}
   requests.post(QRADAR_URL,files=files)

def write_to_json(filename):
   outfile=filename.strip('.log')+'.json'
   outfile=open(outfile,'a')
   infile=open(filename,'r')
   for event in infile.readlines():
       json.dump(event,outfile)
   outfile.close()
   infile.close()

if  __name__=="__main__":

  with open(LAST_EVENT_TIME_FILE,"r") as event_time:
      last_event_time=event_time.readline().rstrip()
  print int(last_event_time)
  cloudflare_logs=list_blobs(BUCKET)
  print cloudflare_logs
  i=0
  for cloudflare_log in cloudflare_logs:
    i=i+1
    print cloudflare_log.name
    filename="cloudflare_log"+str(i)+".log"
    #download_blob(BUCKET,logs_list[1],filename)
    #print logs_list[i]
    if is_file(cloudflare_log.name):
      download_blob(BUCKET,cloudflare_log.name,filename)
      post_to_qradar(filename)
      #print type(filename)
      event_time=open(LAST_EVENT_TIME_FILE,"w")
      event_time.write(str(cloudflare_log.generation))
      event_time.close()
