from pyspark.sql import *
from pyspark.sql.functions import *
import boto3

#print all bucket names
s3 = boto3.resource('s3')
for bucket in s3.buckets.all():
    print(bucket.name)
#/////////////////////////////////
print("in any nucket if u have input folder..listout all objects")
for bucket in s3.buckets.all():
    for obj in bucket.objects.filter(Prefix='input/'):
        print('{0}:{1}'.format(bucket.name, obj.key))

#/////////////////////////////
print("delete one object in specified bucket")
#venukatragadda:input/10000Records.csv
s3.Object('venukatragadda', 'input/10000Records.csv').delete()

#/////////////
print("if u want to delete all objects from s3 perticular bucket")
bucket = s3.Bucket('venukatragadda')
objects = bucket.objects.filter(Prefix = 'input/')

objects_to_delete = [{'Key': o.key} for o in objects if o.key.endswith('.txt') or o.key.endswith('.csv')]

if len(objects_to_delete):
    s3.meta.client.delete_objects(Bucket='venukatragadda', Delete={'Objects': objects_to_delete})
#resource: https://stackoverflow.com/questions/65175454/how-to-delete-multiple-files-and-specific-pattern-in-s3-boto3
#delete bucket if bucket is empty
client = boto3.client('s3')
objects = client.list_objects_v2(Bucket="venuoct30poc")
fileCount = objects['KeyCount']
if fileCount == 0:
 response = client.delete_bucket(Bucket="venuoct30poc")
 print("{} has been deleted successfully !!!".format("venuoct30poc"))
else:
 print("{} is not empty {} objects present".format("venuoct30poc",fileCount))
 print("Please make sure S3 bucket is empty before deleting it !!!")
#resource https://dheeraj3choudhary.com/listcreate-and-delete-s3-buckets-using-python-boto3-script

#above code bucket nonempty so delete all objects from that bucket.
print("deleting all objects within specified bucket")



bucket = s3.Bucket('venuoct30poc')
objects = bucket.objects

objects_to_delete = [o for o in objects]
s3.meta.client.delete_objects(Bucket='venuoct30poc', Delete={'Objects': objects_to_delete})
