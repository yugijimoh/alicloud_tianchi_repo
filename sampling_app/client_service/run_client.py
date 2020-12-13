from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import os
import socket
import json
from urllib import request, parse
import global_var
# process data streams in multiple threads
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()
batch_size = 20000
sc = SparkContext(appName='Tianchi')
#ssc = StreamingContext(sc,2)
self_port = os.environ.get("SERVER_PORT")  # for communication between dockers
# try:
#     #create a stream socket (TCP)
#     s = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
# except (socket.error, msg):
#     logger.error("failed to create socket. error code: {}".format(str(msg)))
# logger.info("Socket created")
# tcp_host = "127.0.0.1"
# tcp_port = 4567
# try:
#     addr = socket.gethostbyname(tcp_host)
#     pass
# except socket.gaierror:
#     logger.error('Hostname could not be resolved.')
#
# s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
#
# s.settimeout(1000)
# try:
#     s.bind((tcp_host, tcp_port))
# except OSError as msg:
#     logger.error("socket.bind() failed - {}".format(msg))
#     s.close()
#     s = None
#     logger.error("TCP server socket closed")
#     raise ConnectionError
# s.bind((tcp_host, tcp_port))



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


# def get_traceid(span):
#
#     return traceid

def map_func(x):
    logger.info("################################## " +str(x)+ " ##########################################")
    s = x.split('|')
    """
    traceId | startTime | spanId | parentSpanId | duration | serviceName | spanName | host | tags
    traceId：全局唯一的Id，用作整个链路的唯一标识与组装
    startTime：调用的开始时间
    spanId: 调用链中某条数据(span)的id
    parentSpanId: 调用链中某条数据(span)的父亲id，头节点的span的parantSpanId为0
    duration：调用耗时
    serviceName：调用的服务名
    spanName：调用的埋点名
    host：机器标识，比如ip，机器名
    tags: 链路信息中tag信息，存在多个tag的key和value信息。格式为key1=val1&key2=val2&key3=val3 比如 http.status_code=200&error=1
    """
    return (s[0], str(s[1]), s[2], s[3], str(s[4]), s[5], s[6], s[7], s[8])

    return s


def has_errors(tags):
    if 'error=1' in tags.lower():
        logger.info(tags)
        return True
    elif 'http.status_code' in tags.lower():
        if '200' not in tags.lower():
            logger.info(tags)
            return True
    return False


def is_under_trace(span,traceid):
    try:
        if span[0:span.index("|")] == traceid:
            return True
        else:
            return False
    except Exception as e:
        return False
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
        url = "http://localhost:" + port + filename
    except Exception as e:
        logger.error("Failed to construct url for data")
        logger.error(e.__traceback__)
        url = None
    finally:
        logger.info("Function 'getDataPath' ended.")
        return url


def notify_finish():
    finish_url = "http://localhost:8002/ready4checksum"
    req = request.Request(url=finish_url)
    logger.info("Ready to do checksum")
    resp = request.urlopen(req)
    logger.info(resp)


# def run_client(port):
#     sc = SparkContext(appName='Tianchi')  # 命名
#     lines = sc.textFile("data.txt").map(lambda x: map_func(x)).cache()  # 导入数据且保持在内存中&#xff0c;其中cache():数据保持在内存中
#     print(lines.collect())
#     # data=flask
#     # get_traceid(data)
#     return "getting data from : "
#
#
# run_client(12)


def run_client(port):
    url_path = get_data_path(port)
    logger.info("The url path is {}".format(url_path))
    if url_path is None:
        return "No data obtained."
    req_data = request.urlopen(url_path)
    logger.info("connection ready, reading data")
    count = 0
    curr_list = []
    batch_num = 0
    curr_rdd = ""
    prev_rdd = ""
    while True:
        count += 1
        data = req_data.readline().decode()
        curr_list.append(data)
        if count % batch_size == 0:
            batch_num += 1
            logger.info("size of curr_rdd: {}".format(len(curr_list)))
            logger.info("type {}, content {}".format(type(curr_list[0]),curr_list[0]))
            curr_rdd = sc.parallelize(curr_list)
            logger.info("Type of curr_rdd is {}".format(type(curr_rdd)))
            logger.info("size of curr_rdd: {}".format(curr_rdd.count()))
            mapped_stream = curr_rdd.map(lambda x: x.split("|"))
            if not prev_rdd == "":
                mapped_stream.union(prev_rdd)
            logger.info("Size of mapped stream is {}".format(mapped_stream.count()))
            # find out spans that with errors
            error_stream = mapped_stream.filter(lambda x: has_errors(x[8]))
            keylist = error_stream.keys().collect()
            logger.info("keylist stream: {}".format(keylist))
            # logger.info("error_stream: {}".format(error_stream.collect()))
            error_span_dict = {}
            for key in keylist:
                spans = curr_rdd.filter(lambda x: is_under_trace(x, key))
                error_span_dict[key] = spans.collect()
            # logger.info("what's in the error_span_dict: {}".format(error_span_dict))
            # find out relative spans with same trace id
            # send error traces to backend for further process
            send_error_traces_to_backend(error_span_dict)
            client_crosscheck_with_backend(keylist,batch_num,port)
            prev_rdd = mapped_stream
            global_var.rdd_bucket[batch_num] = curr_rdd
            curr_list = []
            logger.info("Processed batch num: {}".format(batch_num))
            # if batch_num == 2:
            #
            #     break
        if len(data) <= 0:
            break
    notify_finish()
    return "getting data from : {}".format(url_path)


def send_error_traces_to_backend(data):
    target_url = "http://localhost:8002/senderrortrace"
    # data={"a":[1,2,3],"b":[4,5]}
    data=json.dumps(data).encode("utf-8")
    req = request.Request(url=target_url, method='POST', data=data)
    logger.info("sending error traces")
    resp = request.urlopen(req)
    logger.info(resp)


def client_crosscheck_with_backend(keylist,batchnum,port):
    target_url = "http://localhost:8002/client_crosscheck_with_backend"
    # keylist = json.dumps(keylist).encode("utf-8")
    # batchnum = json.dumps(batchnum).encode("utf-8")
    data = {"keylist": keylist, "batchnum": batchnum, "port":port}
    # data = parse.urlencode(data).encode('utf-8')
    data = json.dumps(data).encode("utf-8")
    req = request.Request(url=target_url, method='POST', data=data)
    logger.info("sending error traces")
    resp = request.urlopen(req)
    logger.info(resp)


def getduplicate(error_id_list,batch_num):

    duplicate_list = []
    for id in error_id_list:
        bucket = global_var.rdd_bucket[batch_num]
        spans = bucket.filter(lambda x: is_under_trace(x, id))
        duplicate_list.append(spans)

    target_url = "http://localhost:8002/senderrortrace"
    data = json.dumps(duplicate_list).encode("utf-8")
    request.Request(url=target_url, method='POST', data=data)
    logger.info("sending duplicate error traces")

