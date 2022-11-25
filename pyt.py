from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
li = [1,5,23,5,7,9,6,54,8,11,10,3,67,8,9,2,3,4,5,7,7,5,2,24,6,7]

def splitevenodd(A):
   evenlist = []
   oddlist = []
   for i in A:
      if (i % 2 == 0):
         evenlist.append(i)
      else:
         oddlist.append(i)

   print("Even lists:", sorted(evenlist))
   print("Odd; lists:", sorted(oddlist))
   evenlist.extend(oddlist)
   print("all", sorted(evenlist))


splitevenodd(li)
