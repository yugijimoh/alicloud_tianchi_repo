import hashlib

def getMd5(traceid):
    m=hashlib.md5()
    m.update(traceid)
    return m.hexdigest()

