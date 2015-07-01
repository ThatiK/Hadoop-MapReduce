# Hadoop-MapReduce
Code snippets for map reduce

MultiplePathMappers :
Given input files in different paths pathA\fileA.txt and PathB\fileB.txt
Mapper A will read and filter fileA.txt and Mapper B will read and filter fileB.txt
This is done using MultipleInputs.addInputPath in config class
For each input file there will be output file (if input has required data)

ChainingJobs:
For this project chainMapper is used for implementing job chaining. Mapper B will take input of Mapper A. 1 single job does this task.
This is implemented in two ways
1)ChainMapper class was not availble CDH5 . Had to impot it from https://repository.cloudera.com/artifactory/repo/
		<dependency>
			<groupId>org.apache.hadoop</groupId>
			<artifactId>hadoop-mapreduce-client-core</artifactId>
			<version>2.0.0-cdh4.3.1</version>
		</dependency>
2)Defining 2 jobs in config file
input schema : id,name,location,salary
MapperA filters by location and MapperB filters MapperA output data by salary

UniqueVisitos:
This project takes logs (schema: user-website) as input and output the number of unique users for a website
This project uses Set to get the unique users for a website
output : website-count

Union:
This project takes files with similar structure data from different paths. There will be 1 mapper and 1 reducer
input schema: dept(id,name)
mapper outputs id as key (can also take record as key) and value(whole record) and sends to reducer. Reducer perform print out the union of those records.

Join:
This project is about joining employee table and department table with departmentId. It uses 2 mappers each reading records of employee and department files respectively from different paths. 
input schema: emp(empId,empName,depatId) ; dept(deptId,deptName)
output: empDept(empId,empName,deptName)
drawbacks: 
	-The logic is applicable only if the department file has only 1 column of value keeping deptId as key.
	-deptName in the reducer can be anywhere in the iterable. Consider large iterable and we had to loop the entire 		 iterable to get the deptName

SecondarySort:
This project is an improvement for Join project. The drawbacks are overcome by overriding HashPartitioner, SortComparator, GroupComparator. Here we are using a composite key (combining deptId and a flag to determine from where deptId came - 0 for department and 1 for employee). 
input schema: emp(empId,empName,depatId) ; dept(deptId,deptName)
output: empDept(empId,empName,deptName)

DistributedCache:
This project is an upgradation of SecondarySort. We are eliminating reducer by using DistributedCache. We are making sure that the department table is available in memory for all data nodes using DistributedCache. Reading this file and pushing it into a hashmap so that we can retrieve the department name based on its id replacing the deptId in the emp records would yield a map only join.
input schema: emp(empId,empName,depatId) ; dept(deptId,deptName)
output: empDept(empId,empName,deptName)
command 1: lookup file in hdfs
hadoop jar /tmp/DistributedCache-0.0.1-SNAPSHOT.jar com.cloudwick.hadoop.DistributedCache.DriverManager -files hdfs:///user/karthik/inputFiles/dept.txt  /user/karthik/inputFiles/emp.txt /user/karthik/outputDC
command 2: lookup file in local
hadoop jar /tmp/DistributedCache-0.0.1-SNAPSHOT.jar com.cloudwick.hadoop.DistributedCache.DriverManager -files /tmp/dept.txt  /user/karthik/inputFiles/emp.txt /user/karthik/outputDC
