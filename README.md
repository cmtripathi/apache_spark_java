This helps you to connect the Apache Spark to the database and read the log file. We should have JDK 8, MySQL database and a sample log file.

- Install the MySQL (I have installed docker image for it). After installing please create a database with name “spark_poc”:
```
CREATE DATABASE spark_poc;
```
- Create a Table by name “Person”:
```
CREATE TABLE Person (
id BIGINT AUTO_INCREMENT PRIMARY KEY,
name VARCHAR(40),
age INT
);
```
- Insert the record in Person:
```
INSERT INTO spark_poc.Person (name, age) VALUES('Will Smith', 25);
INSERT INTO spark_poc.Person (name, age) VALUES('Sardar Singh', 22);
```
- Download the sample log file from the: https://github.com/cmtripathi/apache_spark_java/blob/master/apache_logs.log 

- Now create a maven project with following dependencies:
**Spark** - 
spark-core_2.11
spark-sql_2.11

**Spark uses sone hadoop dependency internally** - 
hadoop-common
hadoop-hdfs
hadoop-yarn-api
hadoop-mapreduce-client-core

**DB connector**
mysql-connector-java

**Log4j**
log4j

The output of the **ReadDBService**
```
+---+------------+---+
| id|        name|age|
+---+------------+---+
|  1|  Will Smith| 25|
|  2|Sardar Singh| 22|
+---+------------+---+
```
```
Content Size Avg: 275240, Min: 0, Max: 69192717
Response code counts: [(404,213), (200,9126), (416,2), (206,45), (304,445), (500,3), (301,164), (403,2)]
Top Endpoints: [(/favicon.ico,807), (/style2.css,546), (/reset.css,538), (/images/jordan-80.png,533), (/images/web/2009/banner.png,516), (/blog/tags/puppet?flav=rss20,488), (/projects/xdotool/,224), (/?flav=rss20,217), (/,197), (/robots.txt,180)]
```
