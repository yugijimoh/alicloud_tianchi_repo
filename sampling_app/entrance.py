from flask import Flask
from flask import request as req
from client_service import run_client
import logging
logger=logging.getLogger()
app = Flask(__name__)
client_port1 = '8001'
client_port2 = '8002'


@app.route('/')
def hello_world():
    return 'Hello, World!'
@app.route('/ready')
def ready():
    return 'ready'

@app.route('/setParameter')
def setParam():
    port=req.args.get("port")
    """
    add function to read file from this port
    """
    res = ""
    logger.info('printing')
    logger.info("as {}".format(port))
    #print(type(port))
    if port == client_port1 or port == client_port2:
        #run client process with the port num
        res = run_client.run_client(port)
        logger.info(res)
    return res

@app.route('/start')
def startapp():
    return 'start'

if __name__ == '__main__':
    app.run()