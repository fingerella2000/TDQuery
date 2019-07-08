# TDQuery

### Purpose

This a free command line tool which can be used to query a database table in Treasure Data with some options to filter the result.

It was built upon td-client-java 0.8.0.

A simple example would be:
**```java -jar TDQuery-1.0.jar bryandb orders```** 
which will return all records in table "orders" from database "bryandb".

There are some options you can use to filter the result, please refer to the usage section.

## Usage
### How to run it from JAR file

1. Download the JAR file TDQuery-1.0.jar from target folder to your local disk.
2. Open command line interface(e.g. cmd.exe in Windows), cd to the you local disk.
3. Run command ```java -jar TDQuery-1.0.jar Database TableName``` to execute a query.
4. Get the below help message by running ```java -jar TDQuery-1.0.jar -h``` or simply run ```java -jar TDQuery-1.0.jar``` without any arguments and options.

```
usage: DBQuery [Options]... DatabaseName, TableName
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
### How to build it from source

1. You will need install Java and Maven.
Below the version used in this program.
JDK 1.8.0_211
Apache Maven 3.6.1

2. Download all the source code from repository.
3. Open command interface(e.g. cmd.exe in Windows), cd to the folder "TDQuery".
4. Run command ```mvn package```
5. Now you will have the executable JAR file generated in the target.
6. Run it as a normal JAR file.

