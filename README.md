# TDQuery

## Purpose

This a free command line tool which can be used to query a database table in Treasure Data with some options to filter the result.
All result will ordered by time(default column) in descending order.

## Usage

### How to run it from JAR file

1. Download the JAR file TDQuery-1.0.jar from target folder to your local disk.
2. Open command line interface(e.g. cmd.exe in Windows), cd to the you local disk.
3. Run command ```java -jar TDQuery-(version).jar bryandb orders``` to execute a query.
4. Get help message by running ```java -jar TDQuery-(version).jar -h``` or simply run ```java -jar TDQuery-(version).jar``` without any arguments.

A simple example would be:
**```java -jar TDQuery-1.0.jar bryandb orders```** 
which will return all records in table "orders" from database "bryandb".

There are some options you can use to filter the result as described as below:

```
usage: DBQuery [Options]... DatabaseName TableName
 Options:
 -c,--column <col1,col2,...>   Optional: The comma separated list of
                               columns to restrict the result to. Return
                               all columns if not specified.
 -e,--engine <engine>          Optional: The query engine, presto or hive.
                               presto by default.
 -f,--format <tsv/csv>         Optional: The output format, csv(comma
                               separated value) or tsv(tab separated
                               value). tsv by default.
 -h,--help                     Tisplay help message
 -l,--limit <number>           Optional: The limit of records returned.
                               Read all records if not specified.
 -m,--min <timestamp>          Optional: The minimum timestamp. NULL by
                               default.
 -M,--MAX <timestamp>          Optional: The maximum timestamp. NULL by
                               default.
```

### Access your own database in Treasure Data

The database name "bryandb" and table name "orders" used in the example is only for demonstrating purpose and it was the builtin data set which are accessed using this tool.

The table "orders" have some fictional purchase order information with the following coloumns:
```
time
orderNumber
customerNumber
orderDate
shippedDate
status
```

Time range from Sunday, January 5, 2003 3:00:00 PM(1041778800 in Unix Timestamp) to Monday, May 30, 2005 3:00:00 PM(1117465200 in Unix Timestamp). You can use this webpage to convert Unix Timestamp to human readable time. (See: https://www.epochconverter.com/)
You will need Unix Timestamp when you want to filter query result by time.

Since you may have your own data already resided in the Treasure Data and you may want to use this program to access it, you can achieve it by build this program from source code and modify the property file. Please see the section about how to build it from source.

After you downloaded the source code successfully, you can modify the file "tdclient.properties" in folder "src\main\resources" using your own apikey. Then continue with the build process to build an executable JAR file. Then you are good to go.

If you have Treasure Data Toolbelt(https://toolbelt.treasuredata.com/) installed in your environment and it is already configured(run command "td -e" from command line to check if you have toolbet installed and configured), then you can simply delete the file "tdclient.properties" from folder "src\main\resources". In this way, the program will load your apikey from "home\.td\td.conf" directly.

Please choose your preferred way.

### How to build it from source

1. You will need install Java and Maven. Below is the version used in this program.

 JDK 1.8.0_211
 
 Apache Maven 3.6.1

2. Download all the source code from repository.
3. Open command interface(e.g. cmd.exe in Windows), cd to the folder "TDQuery".
4. Run command ```mvn package```
5. Now you will have the executable JAR file generated in the target. There will be two JAR files generated, please choose the JAR file with the name ```TDQuery-(version).jar```.
6. Run it as an executable JAR file. The usage is the same as before.

## Tools and libraries used in the program

1. td-client-0.9.0.jar, Java Library for Treasure Data REST API.
2. commons-cli-1.4.jar, Apache commons-cli library used for parsing command line options.
3. logback-classic-1.2.3.jar, Logback library for logging.
4. AllPairs for creating test cases. (https://www.satisfice.com/download/allpairs)

