"""
1d37a8b17db8568b|1589285985482007|3d1e7e1147c1895d|1d37a8b17db8568b|1259|InventoryCenter|/api/traces|192.168.0.2|http.status_code=200&http.url=http://tracing.console.aliyun.com/getOrder&component=java-web-servlet&span.kind=server&http.method=GET
1d37a8b17db8568b|1589285985482015|4ee98bd6d34500b3|3d1e7e1147c1895d|1251|PromotionCenter|getAppConfig|192.168.0.4|http.status_code=200&component=java-spring-rest-template&span.kind=client&http.url=http://localhost:9004/getPromotion?id=1&peer.port=9004&http.method=GET
1d37a8b17db8568b|1589285985482023|2a7a4e061ee023c3|3d1e7e1147c1895d|1243|OrderCenter|sls.getLogs|192.168.0.6|http.status_code=200&component=java-spring-rest-template&span.kind=client&http.url=http://tracing.console.aliyun.com/getInventory?id=1&peer.port=9005&http.method=GET
1d37a8b17db8568b|1589285985482031|243a3d3ca6115a4d|3d1e7e1147c1895d|1235|InventoryCenter|checkAndRefresh|192.168.0.8|http.status_code=200&component=java-spring-rest-template&span.kind=client&peer.port=9005&http.method=GET
"""


import hashlib
from urllib import parse, request
import logging
import json


# from pyspark import SparkContext
# cleanup unexpected sc before initializing
# try:
#     sc.stop()
# except Exception as e:
#     logger.warning("sc.stop executed, no sc found.")
# # initial sc
# sc=SparkContext(appName='backend')
# rdd = sc.parallelize(spans)
# rdd.sortBy(lambda x: x[1])

logger = logging.getLogger()
error_spans = {}  # {traceid:[span1,span2...spann],}
res_dict = {}  # {traceid:md5,}

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


data structure:
receive from pod0 &pod1

error_trace_id_list{id1,id2...}
"""

# def map_func(x):
#     s = x.split('|')
#     tags = []
#     if s[8] is not None:
#         tags = s[8].split('&')
#     return (s[0], int(s[1]), s[2], s[3], int(s[4]), s[5], s[6], s[7], tags)


def get_md5(span_string):
    m = hashlib.md5()
    m.update(span_string)
    return m.hexdigest()


def send_checksum():
    data = bytes(parse.urlencode(res_dict), encoding='utf8')
    finish_url = "http://localhost:8080/api/finished"
    req = request.Request(url=finish_url, data=data, method='POST')
    logger.info("Ready to send result")
    resp = request.urlopen(req)
    logger.warning(resp)


def sort_and_checksum_spans():
    """
    global variable: error_spans: {traceid1:[span1,span2...spann],traceid2:[span1,span2...spann]}

    :return:
    """
    logger.info("now start to sort and checksum..")
    for i in error_spans.keys():
        try:
            spans = error_spans.get(i)
            # split the span value with '|' and sort by timestamp, the second column
            spans.sort(key=lambda x: x.split("|")[1])
            # transform list into string and use \n" as the delimiter
            md5_value = get_md5('\n'.join(spans))
            res_dict[i] = md5_value
        except Exception as e:
            logger.error(e)
        finally:
            pass


def update_error_dict_with_trace_from_client(data):
    """
    get the traces from a post
    data: error_trace [trace_id:span[]]
    :return: nothing
    """
    logger.info("Obtained error trace from client, merging..")
    if type(data) is dict and len(data)>0:
        for k in data.keys():
            es_v = error_spans.get(k)
            """
            if trace_id exists, then extend the span list
            else create key:value with traceid:[span]
            """
            if es_v is not None:
                es_v.extend(data.get(k))
            else:
                error_spans[k] = data.get(k)
    logger.info(error_spans)
    pass