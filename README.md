# spark-optimization

Suppose we want to compose query in which we get for each question also the number of answers to this question for each month. See the query below which does that in a suboptimal way and try to rewrite it to achieve a more optimal plan.

## Here are several ways one can improve performance of a Spark job

1. By picking the right operators
2. Reduce the number of shuffles and the amount of data shuffled
3. Tuning Resource Allocation
4. Tuning the Number of Partitions
5. Reducing the Size of Data Structures
6. Choosing Data Formats


## The result of the original code.

      +-----------+--------------------+--------------------+-----+---+               
      |question_id|       creation_date|               title|month|cnt|
      +-----------+--------------------+--------------------+-----+---+
      |     155989|2014-12-31 20:59:...|Frost bubble form...|    1|  1|
      |     155989|2014-12-31 20:59:...|Frost bubble form...|    2|  1|
      |     155990|2014-12-31 21:51:...|The abstract spac...|    1|  2|
      |     155992|2014-12-31 22:44:...|centrifugal force...|    1|  1|
      |     155993|2014-12-31 22:56:...|How can I estimat...|    1|  1|
      |     155995|2015-01-01 00:16:...|Why should a solu...|    1|  3|
      |     155996|2015-01-01 01:06:...|Why do we assume ...|    1|  2|
      |     155996|2015-01-01 01:06:...|Why do we assume ...|    2|  1|
      |     155996|2015-01-01 01:06:...|Why do we assume ...|   11|  1|
      |     155997|2015-01-01 01:26:...|Why do square sha...|    1|  3|
      |     155999|2015-01-01 02:01:...|Diagonalizability...|    1|  1|
      |     156008|2015-01-01 03:48:...|Capturing a light...|    1|  2|
      |     156008|2015-01-01 03:48:...|Capturing a light...|   11|  1|
      |     156016|2015-01-01 05:31:...|The interference ...|    1|  1|
      |     156020|2015-01-01 06:19:...|What is going on ...|    1|  1|
      |     156021|2015-01-01 06:21:...|How to calculate ...|    2|  1|
      |     156022|2015-01-01 06:55:...|Advice on Major S...|    1|  1|
      |     156025|2015-01-01 07:32:...|Deriving the Cano...|    1|  1|
      |     156026|2015-01-01 07:49:...|Does Bell's inequ...|    1|  3|
      |     156027|2015-01-01 07:49:...|Deriving X atom f...|    1|  1|
      +-----------+--------------------+--------------------+-----+---+

      Processing time: 6.384432077407837 seconds

      == Physical Plan ==
      AdaptiveSparkPlan isFinalPlan=false
      +- Project [question_id#12L, creation_date#14, title#15, month#28, cnt#45L]
         +- BroadcastHashJoin [question_id#12L], [question_id#0L], Inner, BuildRight, false
            :- Filter isnotnull(question_id#12L)
            :  +- FileScan parquet [question_id#12L,creation_date#14,title#15] Batched: true, DataFilters: [isnotnull(question_id#12L)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/daniel/Desktop/Data_Engineering/Spark Optimization/Optimiz..., PartitionFilters: [], PushedFilters: [IsNotNull(question_id)], ReadSchema: struct<question_id:bigint,creation_date:timestamp,title:string>
            +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, bigint, true]),false), [id=#187]
               +- HashAggregate(keys=[question_id#0L, month#28], functions=[count(1)])
                  +- Exchange hashpartitioning(question_id#0L, month#28, 200), ENSURE_REQUIREMENTS, [id=#184]
                     +- HashAggregate(keys=[question_id#0L, month#28], functions=[partial_count(1)])
                        +- Project [question_id#0L, month(cast(creation_date#2 as date)) AS month#28]
                           +- Filter isnotnull(question_id#0L)
                              +- FileScan parquet [question_id#0L,creation_date#2] Batched: true, DataFilters: [isnotnull(question_id#0L)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/daniel/Desktop/Data_Engineering/Spark Optimization/Optimiz..., PartitionFilters: [], PushedFilters: [IsNotNull(question_id)], ReadSchema: struct<question_id:bigint,creation_date:timestamp>

                        
## Option 1 - Enable AQE (Adaptive Query Execution)

Enabling AQE will do the followings.

- Dynamically coalescing shuffle partitions
- Dynamically switching join strategies
- Dynamically optimizing skew joins

      spark.conf.set("spark.sql.adaptive.enabled", "true")


## Option 2 - AQE + Repartition.

Repartition does a full shuffle, creates new partitions, and increases the level of parallelism in the application. Adding one shuffle to the query plan might eliminate two other shuffles. Repartitioning can be performed by specific columns. It would be very useful if there are multiple joins or aggregations on the partitioned columns.

      start_time = time.time()

      spark.conf.set("spark.sql.adaptive.enabled","true")

      answers_month = answersDF.withColumn('month', month('creation_date'))
      answers_month = answers_month.repartition(col("month"))
      answers_month = answers_month.groupBy('question_id', 'month').agg(count('*').alias('cnt'))

      resultDF = questionsDF.join(answers_month, 'question_id').select('question_id', 'creation_date', 'title', 'month', 'cnt')
      resultDF.orderBy('question_id', 'month').show()

      print("Processing time: %s seconds" % (time.time() - start_time))
      print("\n")
      resultDF.explain()

### Result

      +-----------+--------------------+--------------------+-----+---+               
      |question_id|       creation_date|               title|month|cnt|
      +-----------+--------------------+--------------------+-----+---+
      |     155989|2014-12-31 20:59:...|Frost bubble form...|    1|  1|
      |     155989|2014-12-31 20:59:...|Frost bubble form...|    2|  1|
      |     155990|2014-12-31 21:51:...|The abstract spac...|    1|  2|
      |     155992|2014-12-31 22:44:...|centrifugal force...|    1|  1|
      |     155993|2014-12-31 22:56:...|How can I estimat...|    1|  1|
      |     155995|2015-01-01 00:16:...|Why should a solu...|    1|  3|
      |     155996|2015-01-01 01:06:...|Why do we assume ...|    1|  2|
      |     155996|2015-01-01 01:06:...|Why do we assume ...|    2|  1|
      |     155996|2015-01-01 01:06:...|Why do we assume ...|   11|  1|
      |     155997|2015-01-01 01:26:...|Why do square sha...|    1|  3|
      |     155999|2015-01-01 02:01:...|Diagonalizability...|    1|  1|
      |     156008|2015-01-01 03:48:...|Capturing a light...|    1|  2|
      |     156008|2015-01-01 03:48:...|Capturing a light...|   11|  1|
      |     156016|2015-01-01 05:31:...|The interference ...|    1|  1|
      |     156020|2015-01-01 06:19:...|What is going on ...|    1|  1|
      |     156021|2015-01-01 06:21:...|How to calculate ...|    2|  1|
      |     156022|2015-01-01 06:55:...|Advice on Major S...|    1|  1|
      |     156025|2015-01-01 07:32:...|Deriving the Cano...|    1|  1|
      |     156026|2015-01-01 07:49:...|Does Bell's inequ...|    1|  3|
      |     156027|2015-01-01 07:49:...|Deriving X atom f...|    1|  1|
      +-----------+--------------------+--------------------+-----+---+
      only showing top 20 rows

      Processing time: 5.661179780960083 seconds


      == Physical Plan ==
      AdaptiveSparkPlan isFinalPlan=false
      +- Project [question_id#12L, creation_date#14, title#15, month#28, cnt#45L]
         +- BroadcastHashJoin [question_id#12L], [question_id#0L], Inner, BuildRight, false
            :- Filter isnotnull(question_id#12L)
            :  +- FileScan parquet [question_id#12L,creation_date#14,title#15] Batched: true, DataFilters: [isnotnull(question_id#12L)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/daniel/Desktop/Data_Engineering/Spark Optimization/Optimiz..., PartitionFilters: [], PushedFilters: [IsNotNull(question_id)], ReadSchema: struct<question_id:bigint,creation_date:timestamp,title:string>
            +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, bigint, true]),false), [id=#198]
               +- HashAggregate(keys=[question_id#0L, month#28], functions=[count(1)])
                  +- HashAggregate(keys=[question_id#0L, month#28], functions=[partial_count(1)])
                     +- Exchange hashpartitioning(month#28, 200), REPARTITION_BY_COL, [id=#190]
                        +- Project [question_id#0L, month(cast(creation_date#2 as date)) AS month#28]
                           +- Filter isnotnull(question_id#0L)
                              +- FileScan parquet [question_id#0L,creation_date#2] Batched: true, DataFilters: [isnotnull(question_id#0L)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/daniel/Desktop/Data_Engineering/Spark Optimization/Optimiz..., PartitionFilters: [], PushedFilters: [IsNotNull(question_id)], ReadSchema: struct<question_id:bigint,creation_date:timestamp>                        

## Option 3 - AQE + Join by broadcast

Joining two tables is one of the main transactions in Spark. It mostly requires shuffle which has a high cost due to data movement between nodes. If one of the tables is small enough, any shuffle operation may not be required. By broadcasting the small table to each node in the cluster, shuffle can be simply avoided.

      spark.conf.set("spark.sql.adaptive.enabled", "true")

      start_time = time.time()

      answers_month = answersDF.withColumn('month', month('creation_date')).groupBy('question_id', 'month').agg(count('*').alias('cnt'))

      # using broadcast() with "answers_month"
      resultDF_2 = questionsDF.join(broadcast(answers_month), "question_id").select('question_id', 'creation_date', 'title', 'month', 'cnt')
      resultDF_2.orderBy('question_id', 'month').show()

      print("Processing time: %s seconds" % (time.time() - start_time))
      print("\n")
      resultDF_2.explain()

### Result

      +-----------+--------------------+--------------------+-----+---+               
      |question_id|       creation_date|               title|month|cnt|
      +-----------+--------------------+--------------------+-----+---+
      |     155989|2014-12-31 20:59:...|Frost bubble form...|    1|  1|
      |     155989|2014-12-31 20:59:...|Frost bubble form...|    2|  1|
      |     155990|2014-12-31 21:51:...|The abstract spac...|    1|  2|
      |     155992|2014-12-31 22:44:...|centrifugal force...|    1|  1|
      |     155993|2014-12-31 22:56:...|How can I estimat...|    1|  1|
      |     155995|2015-01-01 00:16:...|Why should a solu...|    1|  3|
      |     155996|2015-01-01 01:06:...|Why do we assume ...|    1|  2|
      |     155996|2015-01-01 01:06:...|Why do we assume ...|    2|  1|
      |     155996|2015-01-01 01:06:...|Why do we assume ...|   11|  1|
      |     155997|2015-01-01 01:26:...|Why do square sha...|    1|  3|
      |     155999|2015-01-01 02:01:...|Diagonalizability...|    1|  1|
      |     156008|2015-01-01 03:48:...|Capturing a light...|    1|  2|
      |     156008|2015-01-01 03:48:...|Capturing a light...|   11|  1|
      |     156016|2015-01-01 05:31:...|The interference ...|    1|  1|
      |     156020|2015-01-01 06:19:...|What is going on ...|    1|  1|
      |     156021|2015-01-01 06:21:...|How to calculate ...|    2|  1|
      |     156022|2015-01-01 06:55:...|Advice on Major S...|    1|  1|
      |     156025|2015-01-01 07:32:...|Deriving the Cano...|    1|  1|
      |     156026|2015-01-01 07:49:...|Does Bell's inequ...|    1|  3|
      |     156027|2015-01-01 07:49:...|Deriving X atom f...|    1|  1|
      +-----------+--------------------+--------------------+-----+---+
      only showing top 20 rows

      Processing time: 6.006971120834351 seconds


      == Physical Plan ==
      AdaptiveSparkPlan isFinalPlan=false
      +- Project [question_id#12L, creation_date#14, title#15, month#28, cnt#45L]
         +- BroadcastHashJoin [question_id#12L], [question_id#0L], Inner, BuildRight, false
            :- Filter isnotnull(question_id#12L)
            :  +- FileScan parquet [question_id#12L,creation_date#14,title#15] Batched: true, DataFilters: [isnotnull(question_id#12L)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/daniel/Desktop/Data_Engineering/Spark Optimization/Optimiz..., PartitionFilters: [], PushedFilters: [IsNotNull(question_id)], ReadSchema: struct<question_id:bigint,creation_date:timestamp,title:string>
            +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, bigint, true]),false), [id=#192]
               +- HashAggregate(keys=[question_id#0L, month#28], functions=[count(1)])
                  +- Exchange hashpartitioning(question_id#0L, month#28, 200), ENSURE_REQUIREMENTS, [id=#189]
                     +- HashAggregate(keys=[question_id#0L, month#28], functions=[partial_count(1)])
                        +- Project [question_id#0L, month(cast(creation_date#2 as date)) AS month#28]
                           +- Filter isnotnull(question_id#0L)
                              +- FileScan parquet [question_id#0L,creation_date#2] Batched: true, DataFilters: [isnotnull(question_id#0L)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/daniel/Desktop/Data_Engineering/Spark Optimization/Optimiz..., PartitionFilters: [], PushedFilters: [IsNotNull(question_id)], ReadSchema: struct<question_id:bigint,creation_date:timestamp>



## Option 4 - AQE + Repartition + Join by broadcast

      start_time = time.time()

      spark.conf.set("spark.sql.adaptive.enabled","true")

      answers_month = answersDF.withColumn('month', month('creation_date'))
      answers_month = answers_month.repartition(col("month"))
      answers_month = answers_month.groupBy('question_id', 'month').agg(count('*').alias('cnt'))

      resultDF = questionsDF.join(broadcast(answers_month), "question_id").select('question_id', 'creation_date', 'title', 'month', 'cnt')
      resultDF.orderBy('question_id', 'month').show()

      print("Processing time: %s seconds" % (time.time() - start_time))
      print("\n")
      resultDF.explain()

## Result
      +-----------+--------------------+--------------------+-----+---+               
      |question_id|       creation_date|               title|month|cnt|
      +-----------+--------------------+--------------------+-----+---+
      |     155989|2014-12-31 20:59:...|Frost bubble form...|    1|  1|
      |     155989|2014-12-31 20:59:...|Frost bubble form...|    2|  1|
      |     155990|2014-12-31 21:51:...|The abstract spac...|    1|  2|
      |     155992|2014-12-31 22:44:...|centrifugal force...|    1|  1|
      |     155993|2014-12-31 22:56:...|How can I estimat...|    1|  1|
      |     155995|2015-01-01 00:16:...|Why should a solu...|    1|  3|
      |     155996|2015-01-01 01:06:...|Why do we assume ...|    1|  2|
      |     155996|2015-01-01 01:06:...|Why do we assume ...|    2|  1|
      |     155996|2015-01-01 01:06:...|Why do we assume ...|   11|  1|
      |     155997|2015-01-01 01:26:...|Why do square sha...|    1|  3|
      |     155999|2015-01-01 02:01:...|Diagonalizability...|    1|  1|
      |     156008|2015-01-01 03:48:...|Capturing a light...|    1|  2|
      |     156008|2015-01-01 03:48:...|Capturing a light...|   11|  1|
      |     156016|2015-01-01 05:31:...|The interference ...|    1|  1|
      |     156020|2015-01-01 06:19:...|What is going on ...|    1|  1|
      |     156021|2015-01-01 06:21:...|How to calculate ...|    2|  1|
      |     156022|2015-01-01 06:55:...|Advice on Major S...|    1|  1|
      |     156025|2015-01-01 07:32:...|Deriving the Cano...|    1|  1|
      |     156026|2015-01-01 07:49:...|Does Bell's inequ...|    1|  3|
      |     156027|2015-01-01 07:49:...|Deriving X atom f...|    1|  1|
      +-----------+--------------------+--------------------+-----+---+
      only showing top 20 rows

      Processing time: 5.695613861083984 seconds


      == Physical Plan ==
      AdaptiveSparkPlan isFinalPlan=false
      +- Project [question_id#12L, creation_date#14, title#15, month#28, cnt#45L]
         +- BroadcastHashJoin [question_id#12L], [question_id#0L], Inner, BuildRight, false
            :- Filter isnotnull(question_id#12L)
            :  +- FileScan parquet [question_id#12L,creation_date#14,title#15] Batched: true, DataFilters: [isnotnull(question_id#12L)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/daniel/Desktop/Data_Engineering/Spark Optimization/Optimiz..., PartitionFilters: [], PushedFilters: [IsNotNull(question_id)], ReadSchema: struct<question_id:bigint,creation_date:timestamp,title:string>
            +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, bigint, true]),false), [id=#202]
               +- HashAggregate(keys=[question_id#0L, month#28], functions=[count(1)])
                  +- HashAggregate(keys=[question_id#0L, month#28], functions=[partial_count(1)])
                     +- Exchange hashpartitioning(month#28, 200), REPARTITION_BY_COL, [id=#194]
                        +- Project [question_id#0L, month(cast(creation_date#2 as date)) AS month#28]
                           +- Filter isnotnull(question_id#0L)
                              +- FileScan parquet [question_id#0L,creation_date#2] Batched: true, DataFilters: [isnotnull(question_id#0L)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/daniel/Desktop/Data_Engineering/Spark Optimization/Optimiz..., PartitionFilters: [], PushedFilters: [IsNotNull(question_id)], ReadSchema: struct<question_id:bigint,creation_date:timestamp>
                        
## Conclusion

The original code was optimized from 6.384432077407837 seconds to 5.695613861083984 by using AQE, repartitioning, and broadcasting.
