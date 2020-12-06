import time

"""
1d37a8b17db8568b|1589285985482007|3d1e7e1147c1895d|1d37a8b17db8568b|1259|InventoryCenter|/api/traces|192.168.0.2|http.status_code=200&http.url=http://tracing.console.aliyun.com/getOrder&component=java-web-servlet&span.kind=server&http.method=GET
1d37a8b17db8568b|1589285985482015|4ee98bd6d34500b3|3d1e7e1147c1895d|1251|PromotionCenter|getAppConfig|192.168.0.4|http.status_code=200&component=java-spring-rest-template&span.kind=client&http.url=http://localhost:9004/getPromotion?id=1&peer.port=9004&http.method=GET
1d37a8b17db8568b|1589285985482023|2a7a4e061ee023c3|3d1e7e1147c1895d|1243|OrderCenter|sls.getLogs|192.168.0.6|http.status_code=200&component=java-spring-rest-template&span.kind=client&http.url=http://tracing.console.aliyun.com/getInventory?id=1&peer.port=9005&http.method=GET
1d37a8b17db8568b|1589285985482031|243a3d3ca6115a4d|3d1e7e1147c1895d|1235|InventoryCenter|checkAndRefresh|192.168.0.8|http.status_code=200&component=java-spring-rest-template&span.kind=client&peer.port=9005&http.method=GET
"""


def map_func(x):
    s = x.split('|')
    tags = []
    if s[8] is not None:
        tags = s[8].split('&')
    return (s[0], int(s[1]), s[2], s[3], int(s[4]), s[5], s[6], s[7], tags)
    # return (s[0], [int(s[1]),int(s[2]),int(s[3])]) #返回为+（key,vaklue+）格式&#xff0c;其中key:x[0],value:x[1]且为有三个元素的列表
    # return (s[0],[int(s[1],s[2],s[3])])   #注意此用法不合法


def hasError(x):
    """
    what is x ??? : should be tags
    if tags contains 'error' then return True,
    """
    for i in x:
        if 'error=1' in i.lower():
            return True
    return False


def notSuccess(x):
    """
    x is tags
    :param x:
    :return:
    """
    for i in x:
        if 'http.status_code' in i.lower():
            if '200' in i.lower():
                return False
            return True
    return True


def getTrace(x):
    if hasError(x) or notSuccess(x):
        return True
    return False


def subMax(x, y):
    m = [x[1][i] if (x[1][i] > y[1][i]) else y[1][i] for i in range(3)]
    return ('Maximum subject score', m)


def sumSub(x, y):
    n = [x[1][i] + y[1][i] for i in range(3)]
    # 或者 n = ([x[1][0]+y[1][0],x[1][1]+y[1][0],x[1][2]+y[1][2]])
    return ('Total subject score', n)


def sumPer(x):
    return (x[0], sum(x[1]))


# 停止之前的SparkContext,不然重新运行或者创建工作会失败.另外,只有 sc.stop()也可以,但是首次运行会有误
try:
    sc.stop()
except:
    pass

from pyspark import SparkContext  # 导入模块

sc = SparkContext(appName='Sampling')  # 命名
lines = sc.textFile("data.txt").map(lambda x: map_func(x)).cache()  # 导入数据且保持在内存中&#xff0c;其中cache():数据保持在内存中
count = lines.count()  # 对RDD中的数据个数进行计数，其中RDD一行为一个数据集
print(lines)

# RDD'转换'运算 +（筛选 关键字filter+）
whohaserror = lines.filter(lambda x: getTrace(x[8])).collect()  # 注意:处理的是value列表，也就是x[1]
# whois0 = lines.filter(lambda x: allis0(x[1])).collect()
# sumScore = lines.map(lambda x: (x[0],sum(x[1]))).collect()
#
# #‘动作’运算
# maxScore = max(sumScore,key=lambda x: x[1]) #总分最高者
# minScore = min(sumScore,key=lambda x: x[1]) #总分最低者
# sumSubScore = lines.reduce(lambda x,y: sumSub(x,y))
# avgScore = [x/count for x in sumSubScore[1]]#单科成绩平均值
#
# #RDD key-value‘转换’运算
# subM = lines.reduce(lambda x,y: subMax(x,y))
# redByK = lines.reduceByKey(lambda x,y: [x[i]+y[i] for i in range(3)]).collect() #合并key相同的value值x[0]+y[0],x[1]+y[1],x[2]+y[2]
#
# #RDD'转换'运算
# sumPerSore = lines.map(lambda x: sumPer(x)).collect() #每个人的总分 #sumSore = lines.map(lambda x: (x[0],sum(x[1]))).collect()
# sorted = lines.sortBy(lambda x: sum(x[1])) #总成绩低到高的学生成绩排序
# sortedWithRank = sorted.zipWithIndex().collect()#按总分排序
# first3 = sorted.takeOrdered(3,key=lambda x:-sum(x[1])) #总分前三者


# 限定以空格的形式输出到文件中
import os, shutil

if os.path.exists("./result"):
    shutil.rmtree("./result")
    print("deleted result of previous run")
# first3RDD = sc.parallelize(first3).map(lambda x:str(x[0])+' '+str(x[1][0])+' '+str(x[1][1])+' '+str(x[1][2])).saveAsTextFile("result")

# print(lines.collect())
# print("数据集个数+（行）:",count)
# print("单科满分者:",whohas100)
# print("单科零分者:",whois0)
# print("单科最高分者:",subM)
# print("单科总分:",sumSubScore)
# print("合并名字相同的分数:",redByK)
# print("总分/+（人）",sumPerSore)
# print("最高总分者:",maxScore)
# print("最低总分者:",minScore)
# print("每科平均成绩:",avgScore)
# print("总分倒序:",sortedWithRank)
# print("总分前三者:",first3)
# print(first3RDD)
print(whohaserror)
sc.stop()
