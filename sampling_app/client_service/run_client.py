from pyspark import SparkContext
import os, shutil
from urllib import request, parse
# process data streams in multiple threads
import logging
logger = logging.getLogger()
batch_size = 20000
self_port = os.environ.get("SERVER_PORT")  # for communication between dockers

"""
several things that run client need to do.
1. identify the data source port
2. pull data from the port
3. process the data in either mini batch or stream (find error)
4. keep records for n-1, n batch for tracing back
5. send result to backend for recon by batch
***result pass to backend and get call back result from backend***
6. obtain error trace ids from backend and find them in n-1 and n batch
7. send back the additional trace spans result to backend
6. notify backend if all data processed.

data structure:
static int count 
batch_num: self_port_ + count
error_trace {[trace id,{trace span data}]}
previous_batch [batch_num,{trace span data}]
current_batch [batch_num,{trace span data}]

"""


def has_errors(tags):
    for i in tags:
        if 'http.status_code' in i.lower():
            if '200' in i.lower():
                continue
            return True
        elif 'error=1' in i.lower():
            return True
        else:
            continue
    return False


def find_error():
    """
    Find error traces in current batch and previous batch
    :return: error_trace list: {[trace id,{trace span data}]}
    """
    pass


def find_records_with_trace_list(traceid_list):
    """
    Find records trace id list which is sent from backend
    expose as service for backend
    :param traceid_list: {traceid1,traceid2...traceidn}
    :return: error_trace list: {[trace id,{trace span data}]}
    """


def get_data_path(port):
    url = ""
    try:
        # port = os.environ.get("SERVER_PORT")
        filename = ""
        if port == "8000":
            filename = "/trace1.data"
        elif port == "8001":
            filename = "/trace2.data"
        else:
            return None
        url = "http://localhost:"+port+filename
    except Exception as e:
        logger.error("Failed to construct url for data")
        logger.error(e.__traceback__)
        url = None
    finally:
        logger.info("Function 'getDataPath' ended.")
        return url


def notify_finish():
    finish_url = "http://localhost:8002/ready4checksum"
    req = request.Request(url=finish_url,method='GET', timeout=3600)
    logger.info("Ready to do checksum")
    resp = request.urlopen(req)
    logger.info(resp)


def run_client(port):

    url_path = get_data_path(port)
    if url_path is None:
        return "No data obtained."
    req_data = request.urlopen(url_path)
    while True:
        data = req_data.readline()
        if len(data) < 0:
            break

    return "getting data from : "
