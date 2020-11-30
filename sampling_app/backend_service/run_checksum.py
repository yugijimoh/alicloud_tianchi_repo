import hashlib
from urllib import parse, request
import logging


logger = logging.getLogger()
res_dict = {}

"""
several things that run checksum need to do.
1. pick up data from 2 different data source (source A and source B)
2. obtain error record source A and store them
3. send request to source B for data querying (subset)
4. consolidate data from source A and subset from source B, store into result dict
5. obtain error record source B and store them
6. send request to source A for data querying (subset)
7. consolidate data from source B and subset from source A, store into result dict 
8. send trace error id in current batch in source A to source B for back check
9. send trace error id in current batch in source B to source A for back check
12. receive end signal from front end, sort data
13. calculate checksum for each data in result dict
14. send post back.
"""


def get_md5(traceid):
    m = hashlib.md5()
    m.update(traceid)
    return m.hexdigest()


def send_checksum():
    data = bytes(parse.urlencode(dict), encoding='utf8')
    finish_url = "http://localhost:8080/api/finished"
    req = request.Request(url=finish_url, data=data, method='POST',timeout=3600)
    logger.info("Ready to send result")
    resp = request.urlopen(req)
    logger.warning(resp)


def run_checksum():
    pass
