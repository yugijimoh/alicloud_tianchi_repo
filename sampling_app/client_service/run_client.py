from pyspark import SparkContext
import os,shutil
# process data streams in multiple threads

def run_client(port):
    print(port)
    return "running for port: "+str(port)