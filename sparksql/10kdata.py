from pyspark.sql import *
from pyspark.sql.functions import *
import re
spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
data = "E:\\bigdata\\datasets\\10000Records.csv"
df = spark.read.format("csv").option("header","true").option("sep",",").option("inferschema","true").load(data)
import re

cols=[re.sub('[^a-zA-Z0-1]',"",c.lower()) for c in df.columns]
#df.columns ull get everything in the form of list
#cols = ["EmpID","NamePrefix","FirstName","MiddleInitial","LastName","Gender","EMail","FathersName","MothersName","MothersMaidenName","DateofBirth","TimeofBirth","AgeinYrs","WeightinKgs","DateofJoining","QuarterofJoining","HalfofJoining","YearofJoining","MonthofJoining","MonthNameofJoining","ShortMonth","DayofJoining","DOWofJoining","ShortDOW","AgeinCompanyYears","Salary","LastHike","SSN","PhoneNo","PlaceName","County","City","State","Zip","Region","UserName","Password"]
ndf = df.toDF(*cols)
ndf.createOrReplaceTempView("tab")
#res=spark.sql("select firstname, regexp_replace(email,'gmail','googlemail') email from tab where email like '%gmail%' and gender='F'")
res=spark.sql("select *, concat_ws(' ',nameprefix,firstname,middleinitial,lastname) fullname from tab")\
    .drop("nameprefix","firstname","middleinitial","lastname")
res1=res.select(sorted(res.columns)).withColumn("repeat",repeat(col("Gender"),5))\
    .withColumn("rev",reverse(col("fullname")))
#|Mrs. Serafina I Bumgarner
#.srM anifarea I renragmuB
res1.show(truncate=False)




#venu goutami .... original data
#imatuog unev..if u apply reverse function u ll get like this
#unev imatuog

#add a new column use .. withColumn
#if that column doesnt exists create new column. if column already exists update that column
