#!/usr/bin/python
import argparse
import logging
import requests
import sys
import requests
import datetime
import re
import time
import pylibmc
from pprint import pprint
from datetime import date, timedelta

from threading import Thread
from Queue import Queue
from cloudlibrary import CloudLibrary
from boopsieoutputadapter import BoopsieOutputAdapter

try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO

try:
    # the json module was included in the stdlib in python 2.6
    # http://docs.python.org/library/json.html
    import json
except ImportError:
    # simplejson 2.0.9 is available for python 2.4+
    # http://pypi.python.org/pypi/simplejson/2.0.9
    # simplejson 1.7.3 is available for python 2.3+
    # http://pypi.python.org/pypi/simplejson/1.7.3
    import simplejson as json

# set logging for debug
try:
    import http.client as http_client
except ImportError:
    # Python 2
    import httplib as http_client

# http_client.HTTPConnection.debuglevel = 1

parser  = argparse.ArgumentParser()
parser.add_argument("-p", "--pin", dest="pin", help="pin / customer code")
parser.add_argument("-t", "--threads", dest="threads", help="count of worker threads; default 3", default=3, type=int)
parser.add_argument("-l", "--payload_size", dest="threshold", help="how many marc records to bundle for zappa; default 1000", default=1000, type=int)
parser.add_argument("-a", "--access_key", dest="access_key", help="Access Key/Account Key provided by CloudLibrary")
parser.add_argument("-i", "--library_id", dest="library_id", help="Library/Account ID provided by CloudLibrary")
#parser.add_argument("-z", "--zappa", dest="zappa", help="the zappa worker hostname - defaults to [https://py52n493ya.execute-api.us-west-2.amazonaws.com/dev/job]", default="https://py52n493ya.execute-api.us-west-2.amazonaws.com/dev/job")
args     = parser.parse_args()

def writer_construct():
  logging.debug("constructing writer...")
  return JSONWriter( StringIO() )

def writer_destroy(writer):
  logging.debug("destroying writer...")
  #writer.file_handle.close()
  #writer.close()
  #logging.debug("ok")
  return None

if __name__ == '__main__':

  logger        = logging.getLogger()
  logger.setLevel(logging.INFO)

  #----------------------------------------------------------------
  # file logging
  #logging.basicConfig(filename='marc_job_controller.log', level=logging.INFO)
  #----------------------------------------------------------------
  # stderr logging
  myhandler = logging.StreamHandler()  # writes to stderr
  myformatter = logging.Formatter(fmt='%(levelname)s: %(message)s')
  myhandler.setFormatter(myformatter)
  logger.addHandler(myhandler)
  requests_log = logging.getLogger("requests.packages.urllib3")
  requests_log.setLevel(logging.INFO)
  requests_log.propagate = True
  #----------------------------------------------------------------

  #----------------------------------------------------------------
  # CONFIG
  num_worker_threads  = args.threads
  threshold           = args.threshold
  MEMCACHED_HOST      = "AWS_MEMCACHED_HOSTNAME"
  #----------------------------------------------------------------

  loop_count       =    0
  record_count     =    0
  seen             =    set()
  uniq             =    []


  #----------------------------------------------------------------
  #----------------------------------------------------------------
  # RETRIEVE ID'S: function to get all ids.
  # These ids will get populated into the retrieve_item_detail queue.
  def _retrieve_ids():
    while True:
      payload = retrieve_ids_q.get()
      if payload is None:
        break
      startdate, enddate = str(payload).split('::')

      get_library_events_uri = "/cirrus/library/" + args.library_id + "/data/cloudevents?startdate=" + startdate + "&enddate=" + enddate
      signature = cloudlibrary.sign_request("GET", get_library_events_uri)
      json_response = cloudlibrary.get_request(get_library_events_uri, signature)

      data = json.loads(json_response.content)
      events_arr = data["Events"]

      for event in events_arr:
        #add ItemId's to retrieve_item_detail queue.
        if not ( str(event['ItemId']) ):
          continue
        if event['ItemId'] is None:
          continue

        if ( str(event['ItemId']) )  not in seen:
          uniq.append(str(event['ItemId']))
          seen.add(str(event['ItemId']))
          retrieve_item_detail_q.put( str( event['ItemId'] ) )

      retrieve_ids_q.task_done()

  #----------------------------------------------------------------

  # RETRIEVE ITEM DETAILS: get item details using ItemId.
  # item details will get stored in memcached
  def _retrieve_item_detail():

    mc = pylibmc.Client(["AWS_MEMCACHED_HOSTNAME"])
      binary       =   True,
      behaviors    =   {"tcp_nodelay": True, "ketama": True})
  
    while True:
      item_id = retrieve_item_detail_q.get()
      if item_id is None:
        break
      if not (item_id):
        continue

      item_details_json = mc.get(item_id)
      data = json.loads(item_details_json)

      try:
        if (not item_details_json) and (not data or data['demco_error']):
          get_item_details_uri = "/cirrus/library/" + args.library_id + "/item/" + item_id

          signature = cloudlibrary.sign_request("GET", get_item_details_uri)
          json_response = cloudlibrary.get_request(get_item_details_uri, signature)

          if not json_response.content:
            pprint("NO JSON RETURNED, Setting error!")
            json_response.content = '{"demco_error":"Demco error: failed to return JSON blob"}'

          #store json to memcache
          mc.set(item_id, json_response.content)
      except:  # includes simplejson.decoder.JSONDecodeError
        logging.info("========ERROR: DID NOT ENTER BLOCK W/ ID '" + item_id +"'")

      #TODO: print to tsv / run boop adapter
  #----------------------------------------------------------------
  #----------------------------------------------------------------

  #----------------------------------------------------------------
  #----------------------------------------------------------------
  # RETRIEVE ITEM DETAILS QUEUE
  # Queue for managing _retrieve_item_detail function.

  retrieve_item_detail_q = Queue()
  num_worker_threads = args.threads #default is 3
  retrieve_item_detail_threads  =    []
  for i in range(num_worker_threads):
    retrieve_item_detail_worker = Thread(target=_retrieve_item_detail)
    retrieve_item_detail_worker.daemon = True
    retrieve_item_detail_worker.start()
    retrieve_item_detail_threads.append(retrieve_item_detail_worker)
  #----------------------------------------------------------------
  # RETRIEVE ID QUEUE
  # Queue for managing _retrieve_ids function.

  retrieve_ids_q = Queue()
  num_worker_threads    =    args.threads #default is 3
  retrieve_ids_threads  =    []
  
  for i in range(num_worker_threads):
    retrieve_ids_worker = Thread(target=_retrieve_ids)
    retrieve_ids_worker.daemon = True
    retrieve_ids_worker.start()
    retrieve_ids_threads.append(retrieve_ids_worker)
  #----------------------------------------------------------------
  #----------------------------------------------------------------

  #create cloudlibrary object w/ args
  cloudlibrary = CloudLibrary(args)

  #create cloudlibrary object w/ args
  boopsie_output_adapter = BoopsieOutputAdapter(args)

  #only way to get full list of records is to do a call for every day and build the list...
  #we can use the get_library_purchase_count_uri endpoint to find what day to start
  #building the list (so we don't have to use an arbiturary date)
  current_date = datetime.datetime.utcnow().strftime("%Y-%m-%d")
  get_library_purchase_count_uri = "/cirrus/library/" + args.library_id + "/data/purchase/count?startDate=" + "1990-05-05" + "&endDate=" + current_date

  signature = cloudlibrary.sign_request("GET", get_library_purchase_count_uri)
  json_response = cloudlibrary.get_request(get_library_purchase_count_uri, signature)
  #logging.info(json_response.content)

  data = json.loads(json_response.content)

  #TODO: should loop through all the dates and grab the earliest
  #(instead of the first in the array, just to be safe)
  start_date = re.sub(r'T.*$', '', data[0]["StartDateInUTC"])
  logging.info("start date: " + start_date)
  
  if not re.match('^\d\d\d\d-\d\d-\d\d$', start_date):
    raise Exception('start date not properly formatted!')
  
  #capture year, month, day
  start_date = "2017-10-01"
  start_yr, start_mth, start_day = re.match(r"(\d\d\d\d)-(\d\d)-(\d\d)", start_date).groups()
  current_yr, current_mth, current_day = re.match(r"(\d\d\d\d)-(\d\d)-(\d\d)", current_date).groups()
  
  d1 = date(int(start_yr), int(start_mth), int(start_day))  # start date (first 3mcl purchase by library)
  d2 = date(int(current_yr), int(current_mth), int(current_day))  # end date (current day)
  
  delta = d2 - d1         # timedelta in days from start date to end date
  #logging.info(delta)
  
  for day_number in range(delta.days + 1): #loop through total num of days (delta.days + 1), adding each query to queue
    #we can get 3m id's for 2 days per call, so skip every other
    if day_number % 2 != 0:
        continue

    #populate the retrieve_ids_q with string that contains start and end date, format: YYYY-MM-DD
    retrieve_ids_q.put( str(d1 + timedelta(days=day_number)) + "::" + str(d1 + timedelta(days=day_number+1)) )

  #this is BLOCKING until all retrieve_ids_q's have completed
  retrieve_ids_q.join()

  #----------------------------------------------------------------
  #RETRIEVE IDS THREAD MANAGEMENT
  #----------------------------------------------------------------
  for i in range(num_worker_threads):
    retrieve_ids_q.put(None)
  for thread in retrieve_ids_threads:
    thread.join()
  #----------------------------------------------------------------


  #BLOCKING until all retrieve_item_detail_q's have completed
  retrieve_item_detail_q.join()


  #----------------------------------------------------------------
  #RETRIEVE ITEM DETAIL THREAD MANAGEMENT
  #----------------------------------------------------------------
  for i in range(num_worker_threads):
    retrieve_item_detail_q.put(None)
  for thread in retrieve_item_detail_threads:
    thread.join()
  #----------------------------------------------------------------

  logging.info("Done... verify all threads are accounted for")
