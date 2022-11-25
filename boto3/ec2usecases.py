
import boto3

#create ec2 instances
ec2= boto3.resource('ec2',region_name ="ap-south-1")

#ImageId changed based on os .. pls check ec2 create .. right hand side
'''ubuntu...ami-062df10d14676e201
   windows...ami-08bd8e5c51334492e
   redhat ...ami-069d9fecd19e7ed40
   '''
instances = ec2.create_instances(
        ImageId="ami-062df10d14676e201",
        MinCount=1,
        MaxCount=2,
        InstanceType="t2.micro",
        KeyName="venuppk")
import time
time.sleep(5)
ids = ['i-05247786cc667dd9a', 'i-09ee8c8b17585b7ba']
#delete multiple ec2 instances.
#ec2.instances.filter(InstanceIds=ids).stop()
ec2.instances.filter(InstanceIds=ids).terminate()

#instead of specified few instances specify all instances need to terminate
ec2.instances.terminate()

