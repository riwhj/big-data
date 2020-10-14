#จงแสดงย่านที่มีการเปิดAirbnb(บ้านพัก)มากที่สุดและเป็นบ้านพักประเภทอะไร ราคาเฉลี่ยเท่าไร

from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc =SparkContext(conf=conf)

def parseline(line):
    fields =line.split(',')
    neighbourhood =fields[5]
    roomtype =fields[8]
    price = float(fields[9])
    return (neighbourhood,roomtype,price)

lines =sc.textFile("file:///SparkCourse/big-data/AB_NYC_2019.csv")
rdd =lines.map(parseline)
neighbourhoodofroomtype = rdd.map(lambda x:(x[0],x[1]))
#totalsByNeighbourhoodofroom_type = rdd.reduceByKey(lambda x,y: x+y)

print(neighbourhoodofroomtype.collect())
