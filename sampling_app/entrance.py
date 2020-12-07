from flask import Flask, send_file, Response
from flask import request as req
from client_service.run_client import run_client, send_error_traces
from backend_service.run_checksum import sort_and_checksum_spans, send_checksum, update_error_dict_with_trace_from_client
from logging.config import dictConfig
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
    sort_and_checksum_spans()
    send_checksum()
    return 'notified'


@app.route('/senderrortrace',methods=['POST'])
def senderrortrace():
    trace = req.get_data()
    app.logger.info("type of trace is: {}".format(type(trace)))
    app.logger.info(trace)
    jtrace = json.loads(trace)
    update_error_dict_with_trace_from_client(jtrace)
    return 'notified'


@app.route('/senderror')
def senderror():
    """
    this method is just to simulate client send trace to backend.
    """
    send_error_traces()
    return 'notified'


@app.route('/trace1.data')
def simulate_download_trace1():
    """
    this method is just to simulate client send trace to backend.
    """
    app.logger.info("Ready to send trace1.data")
    def send_file():
        store_path = "/Users/DL/Documents/alicloud/trace1.data"
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

@app.route('/client_crosscheck_with_backend')
def crosscheck(your_port,data):
    crosscheck(your_port,data)

if __name__ == '__main__':
    app.run(debug=True,port=port)
