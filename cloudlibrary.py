import json
import logging
import requests
import os
import datetime
import base64
import hmac
import argparse
import hashlib
from pprint import pprint

# encoding=utf8
import sys
reload(sys)
sys.setdefaultencoding('utf8')

# set logging for debug
try:
    import http.client as http_client
except ImportError:
    # Python 2
    import httplib as http_client

class CloudLibrary:

  logger        = logging.getLogger()
  logger.setLevel(logging.WARN)

  #----------------------------------------------------------------
  # localhost logging
  myhandler = logging.StreamHandler()  # writes to stderr
  myformatter = logging.Formatter(fmt='%(levelname)s: %(message)s')
  myhandler.setFormatter(myformatter)
  #----------------------------------------------------------------
  
  http_client.HTTPConnection.debuglevel = 0

  #----------------------------------------------------------------
  # CONFIGURATION
  BASE                                = 'CL_BASE_URL'
  #----------------------------------------------------------------

  def __init__(self,args):
    self.access_key = args.access_key
    self.library_id = args.library_id
    logging.info("access_key: " + self.access_key)

  def _get_timestamp(self):
    return datetime.datetime.utcnow().strftime("%a, %d %b %Y %H:%M:%S GMT")

  def get_request(self, uri, signature):
    req_headers = {
      '3mcl-Datetime': self._get_timestamp(),
      '3mcl-Authorization': 'TOKEN' + self.library_id + ':' + signature,
      '3mcl-APIVersion' : '2.0',
      'Content-Type': 'text/xml; charset=utf-8',
      'Accept' : 'application/json'
    }
    logging.info("request url: " + self.BASE + uri)
    res = requests.get(self.BASE + uri,  headers=req_headers)
    #print vars(res)
    if res.status_code != 200:
      raise Exception("failed - {0}".format(r.json()['Error'][0]['Message']))

    return res

  def sign_request(self, verb, uri):
    data_to_sign = self._get_timestamp() + "\n" + verb + "\n" + uri
    digest = hmac.new(self.access_key, msg=data_to_sign, digestmod=hashlib.sha256).digest()

    signature = base64.b64encode(digest)
    return signature
