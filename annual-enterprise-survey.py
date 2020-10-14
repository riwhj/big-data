#แสดงว่าแต่ละปีมีวงเงินเฉลี่ยเท่าไรและแสดงว่าบริษัทไหนรับผิดชอบมากที่สุด
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc =SparkContext(conf=conf)

def parseline(line):
    fields =line.split(',')
    year=fields[0]
    Variable_name =fields[6]
    value=fields[8]
    return (year,Variable_name,value)

lines =sc.textFile("file:///SparkCourse/annual-enterprise-survey-2019.csv")
rdd =lines.map(parseline)
yearOfVariable_name = rdd.map(lambda x:(x[0],x[1])).reduceByKey(lambda x,y:(x+y))

print(yearOfVariable_name.collect())