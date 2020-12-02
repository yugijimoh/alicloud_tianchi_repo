from flask import Flask
from flask import request as req
from client_service.run_client import run_client
from backend_service.run_checksum import run_checksum
import logging,os
logger = logging.getLogger()
app = Flask(__name__)
self_port = os.environ.get("SERVER_PORT")
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
    logger.info('printing')
    logger.info("as {}".format(port))
    # print(type(port))
    if port == client_port1 or port == client_port2:
        # run client process with the port num
        res = run_client(port)
        logger.info(res)
    return res


@app.route('/start')
def startapp():
    return 'start'


@app.route('/ready4checksum')
def ready4checksum():
    run_checksum()
    return 'notified'


if __name__ == '__main__':
    app.run(debug=True,port=port)
