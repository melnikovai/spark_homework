# Home task for Apache Spark interview
### General requirements 
- Scala > 2.11
- Apache Spark > 3
- SQL or Dataframe API
- unit tests for all UDFs
- no restrictions regarding build tools (SBT or gradle preferable)
- It is preferable to solve tasks in sequential order
- Clone the repo and create PR for solution 
## Task 1. Dataset analysis
Folder `/data` contains 2 csv samples - small dataset with URLs about Apache Spark and countries IDs which videos we are interested in (1 means we are interested, 0 means no).
### Requirements
- Extract video's id from URL (`v` or `jumpid` keys). Please, use UDF and [scala-uri](https://github.com/lemonlabsuk/scala-uri) module for it.
- Extract domain and subpath for resource into `domain` and `subpath` columns
- Filter rows so that result dataset would contain videos only from given countries. Solution must be highly performant
- Filling gaps. All string columns must be filled with `unknown` literal, numeric columns must be filled with `NULL`
- Additional columns. If video marked as `short` new column should contain `short` for it, `video` the otherwise
- Video length column must be converted from total seconds format to `MM:ss` format

## Task 2. Monitoring
### Requirements
Company uses propriatary monitorng system, that doesn't have Spark connector. We need to send data ourself to that server so we need a custom listener of the Spark instance.
Please, implement custom QueryExecutionListener that will output CPU usage, written rows and allocated heap RAM that required to execute Task 1.
Since it is prototype for the real listener, STDOUT as output is good enough at this stage. Result string format is irrelevant since the metrics will be compacted in JSON in real listener. 

## Completion
### general
- latest spark version - 3.5.0
- solution implemented in spark DataFrame API
- tested on a pure spark distribution in local mode
### build
The project is built with sbt with an assembly plugin.
To build the fat jar just use
```sbt assembly```
### launch
To run this application you have to provide three parameters
- --urls - path to csv with urls
- --countries - path to csv with countries
- --output - path of the output dir
Example usage
```$SPARK_HOME/bin/spark-submit --class pl.wisniewskimic.spark.task.App /home/mwisniewski/workspace/spark_homework/target/scala-2.12/spark_homework-assembly-0.1.0-SNAPSHOT.jar --urls /home/mwisniewski/workspace/spark_homework/data/urls.csv --countries /home/mwisniewski/workspace/spark_homework/data/countries.csv --output /home/mwisniewski/spark_answer```