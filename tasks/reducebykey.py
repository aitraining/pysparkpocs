from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
data="E:\\bigdata\\datasets\\donations.csv"
erdd=spark.sparkContext.textFile(data)
skip=erdd.first()
res=erdd.filter(lambda x:x!=skip).map(lambda x:x.split(",")).map(lambda x:(x[0],int(x[2]))).reduceByKey(lambda x,y:x+y)
#reduceByKey ... based on the same Key , process the values
'''let eg .. venu,2000, venu,2000 .. both key same so process value
means venu, x, venu,y.... venu, (lambda x,y:x+y).. venu,4000 similarly
venkar,9000 and venkat,5000 ..... venkat, (9000+5000) .. 14000
('venkat', 9000)
('venu', 2000)
('venu', 2000)
('anu', 3000)
('venkat', 5000)
'''

for x in res.collect():
    print(x)