from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
host="jdbc:redshift://redshiftcluster.cga8fbgcpfol.ap-south-1.redshift.amazonaws.com:5439/dev"
df=spark.read.format("jdbc").option("url",host).option("user","ruser").option("password","Rpassword.1").option("dbtable","sales").option("driver","com.amazon.redshift.jdbc.Driver").load()
#df.show()
ndf=df.where(col("commission")>500)
#ndf=df.select([count(when(col(i).isNull(),i)).alias(i) for i in df.columns])
ndf.show()
ndf.write.mode("append").format("jdbc").option("url",host).option("user","ruser").option("password","Rpassword.1").option("dbtable","sales1").option("driver","com.amazon.redshift.jdbc.Driver").save()
