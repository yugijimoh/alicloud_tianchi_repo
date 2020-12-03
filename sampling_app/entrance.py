from flask import Flask
from flask import request as req
from client_service.run_client import run_client,send_error_traces
from backend_service.run_checksum import run_checksum,get_error_traces_from_client
import logging,os,json
from logging.config import dictConfig
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
if self_port and len(self_port)==4:
    port=self_port

@app.route('/')
def hello_world():
    return 'Hello, World!'


@app.route('/ready')
def ready():
    return 'ready'


@app.route('/setParameter')
def set_param():
    port = req.args.get("port")
    """
    add function to read file from this port
    """
    res = ""
    app.logger.info('printing')
    app.logger.info("as {}".format(port))
    # print(type(port))
    if not port == self_port :
        # run client process with the port num
        res = run_client(port)
        app.logger.info(res)
    return res


@app.route('/start')
def startapp():
    return 'start'


@app.route('/ready4checksum')
def ready4checksum():
    run_checksum()
    return 'notified'


@app.route('/senderrortrace',methods=['POST'])
def senderrortrace():
    trace = req.get_data()
    app.logger.info("type of trace is: {}".format(type(trace)))
    app.logger.info(trace)
    jtrace = json.loads(trace)
    get_error_traces_from_client(jtrace)
    return 'notified'


@app.route('/senderror')
def senderror():
    send_error_traces()
    return 'notified'

if __name__ == '__main__':
    app.run(debug=True,port=port)
