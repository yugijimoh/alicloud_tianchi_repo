from flask import Flask, send_file, Response
from flask import request as req
from client_service.run_client import run_client, send_error_traces_to_backend, getduplicate
from backend_service.run_checksum import sort_and_checksum_spans, send_checksum, \
    update_error_dict_with_trace_from_client, crosscheck
from logging.config import dictConfig
import global_var
import os
import json
app = Flask(__name__)
self_port = os.environ.get("SERVER_PORT")
dictConfig({
    'version': 1,
    'formatters': {'default': {
        'format': '[%(asctime)s] %(levelname)s in %(module)s: %(message)s',
    }},
    'handlers': {'wsgi': {
        'class': 'logging.StreamHandler',
        'formatter': 'default'
    }},
    'root': {
        'level': 'DEBUG',
        'handlers': ['wsgi']
    }
})
port = 8002
print("self_port is {}".format(self_port))
if self_port and len(self_port) == 4:
    port = self_port


@app.route('/')
def hello_world():
    return 'Hello, World!'


@app.route('/ready')
def ready():
    return 'ready'


@app.route('/setParameter')
def set_param():
    data_port = req.args.get("port")
    """
    add function to read file from this port
    """
    res = ""
    app.logger.info("data port is {}".format(data_port))
    # print(type(port))

    # run client process with the port num
    res = run_client(data_port)
    app.logger.info(res)
    return res


@app.route('/start')
def startapp():
    return 'start'


@app.route('/ready4checksum')
def ready4checksum():
    global_var.ready_for_checksum_cnt += 1
    if global_var.ready_for_checksum_cnt == 2:
        sort_and_checksum_spans()
        # send_checksum()
    return 'notified'


@app.route('/senderrortrace',methods=['POST'])
def senderrortrace():
    trace = req.get_data()
    app.logger.info("len of trace is: {}".format(len(trace)))
    jtrace = json.loads(trace)
    update_error_dict_with_trace_from_client(jtrace)
    return 'notified'


@app.route('/senderror')
def senderror():
    """
    this method is just to simulate client send trace to backend.
    """
    send_error_traces_to_backend()
    return 'notified'


@app.route('/trace1.data')
def simulate_download_trace1():
    """
    this method is just to simulate client send trace to backend.
    """
    app.logger.info("Ready to send trace1.data")
    def send_file():
        store_path = "D:/mygitwork/alicloud_tianchi_repo/trace1.data"
        with open(store_path, 'rb') as targetfile:
            while True:
                # data = targetfile.read(20 * 1024 * 1024)  # 每次读取20M
                data = targetfile.readline()
                if not data or len(data) < 1:
                    break
                yield data
    rp = Response(send_file(), content_type='application/octet-stream')
    rp.headers["Content-disposition"] = 'attachment; filename=%s' % 'trace1.data'
    return rp
@app.route('/trace2.data')
def simulate_download_trace2():
    """
    this method is just to simulate client send trace to backend.
    """
    app.logger.info("Ready to send trace1.data")
    def send_file():
        store_path = "D:/mygitwork/alicloud_tianchi_repo/trace2.data"
        with open(store_path, 'rb') as targetfile:
            while True:
                # data = targetfile.read(20 * 1024 * 1024)  # 每次读取20M
                data = targetfile.readline()
                if not data or len(data) < 1:
                    break
                yield data
    rp = Response(send_file(), content_type='application/octet-stream')
    rp.headers["Content-disposition"] = 'attachment; filename=%s' % 'trace1.data'
    return rp

@app.route('/client_crosscheck_with_backend',methods=['POST'])
def client_crosscheck_with_backend():
    reqJson = req.json
    port = reqJson['port']
    error_id_list = reqJson['keylist']
    batch_num = reqJson['batchnum']

    crosscheck(port, error_id_list, batch_num)

@app.route('/get_duplicate_error_trace',methods=['POST'])
def get_duplicate_error_trace():
    reqJson = req.json
    error_id_list = reqJson['error_id_list']
    batch_num = reqJson['batch_num']
    # duplicate_list =
    getduplicate(error_id_list, batch_num)
    # rp = Response(send_file(), content_type='application/octet-stream')
    # rp.data['duplicate_list']=json.dumps(duplicate_list).encode("utf-8");
    return "ok"

if __name__ == '__main__':
    app.run(debug=True,port=port)
