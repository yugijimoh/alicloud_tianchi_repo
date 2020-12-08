a={"a":[1,2,3],"b":[3,4,5]}
b={"b":[7,8],"c":[9.0]}
for i in b.keys():
    print (i)
    g=a.get(i)
    if g is not None:
        g.extend(b.get(i))
    else:
        a[i]=b.get(i)

print(a)
print(b)


spans=[
"1d37a8b17db8568b|1589285985482007|3d1e7e1147c1895d|1d37a8b17db8568b|1259|InventoryCenter|/api/traces|192.168.0.2|http.status_code=200&http.url=http://tracing.console.aliyun.com/getOrder&component=java-web-servlet&span.kind=server&http.method=GET",
"1d37a8b17db8568b|1589285985482015|4ee98bd6d34500b3|3d1e7e1147c1895d|1251|PromotionCenter|getAppConfig|192.168.0.4|http.status_code=200&component=java-spring-rest-template&span.kind=client&http.url=http://localhost:9004/getPromotion?id=1&peer.port=9004&http.method=GET",
"1d37a8b17db8568b|1589285985482023|2a7a4e061ee023c3|3d1e7e1147c1895d|1243|OrderCenter|sls.getLogs|192.168.0.6|http.status_code=200&component=java-spring-rest-template&span.kind=client&http.url=http://tracing.console.aliyun.com/getInventory?id=1&peer.port=9005&http.method=GET",
"1d37a8b17db8568b|1489285985482031|243a3d3ca6115a4d|3d1e7e1147c1895d|1235|InventoryCenter|checkAndRefresh|192.168.0.8|http.status_code=200&component=java-spring-rest-template&span.kind=client&peer.port=9005&http.method=GET"
]


spans.sort(key=lambda x: x.split("|")[1])
print(spans)

def has_errors(tags):
    if 'error=1' in tags.lower():
        print("error=1")
        return True
    elif 'http.status_code' in tags.lower():
        if '200' not in tags.lower():
            print(" not 200")
            return True
    return False

print(has_errors("http.status_code=200&http.url=http://tracing.console.aliyun.com/getOrder&component=java-web-servlet&span.kind=server&http.method=GET"))

print(has_errors("http.status_code=200&component=java-spring-rest-template&span.kind=client&http.url=http://tracing.console.aliyun.com/createOrder?id=4&peer.port=9002&http.method=GET&error=1"))


s1="1d37a8b17db8568b|1589285985482007|3d1e7e1147c1895d|1d37a8b17db8568b|1259|InventoryCenter|/api/traces|192.168.0.2|http.status_code=200&http.url=http://tracing.console.aliyun.com/getOrder&component=java-web-servlet&span.kind=server&http.method=GET"
s2=s1[0:s1.index("|")]
print(s2)
print(s1.replace("\n",''))