# DRAFT.  Don't start yet!

# P5 (3% of grade): Spark And Hive

## Overview

In P5, you'll use Spark to analyze competitive programming problems and their solutions. You'll load your data into Hive tables and views for easy querying. The main table contains numerous IDs in columns; you'll need to join these with other tables or views to determine what these IDs mean.

**Important:** You'll answer 10 questions in P5. Write each question number and text (e.g., "#q1: ...") as a comment in your notebook before each answer so we can easily find and grade your work.

Learning objectives:

- Use Spark's RDD, DataFrame, and SQL interfaces to answer questions about data
- Load data into Hive for querying with Spark
- grouping and optimizing queries
- Use PySpark's machine learning API to train a decision tree

Before starting, please review the [general project directions](../projects.md).

## Corrections/Clarifications

- 3/15/2025: Fixed p5-base dockerfile and added in note to not include the datasets in your submission.
- 3/17/2025: Added hint for boss and worker
- 3/18/2025: Updated autobadger to fix questions 8-10. Should be on version **0.1.11**.
- 3/19/2025: Updated autobadger multiple times for test fixes. Should be on version **0.1.14**.

## Setup

Copy these files from the project into your repository:

- `p5-base.Dockerfile`
- `namenode.Dockerfile`
- `notebook.Dockerfile`
- `datanode.Dockerfile`
- `docker-compose.yml`
- `build.sh`
- `get_data.py`
- `.gitignore`

Create a Python virtual environment and install the [datasets library](https://huggingface.co/docs/datasets/en/index):

```sh
pip3 install datasets==3.3.2
```

Create the directory structure with:

```sh
mkdir -p nb/data
```

Run the provided `get_data.py` script to download the [DeepMind CodeContests dataset](https://huggingface.co/datasets/deepmind/code_contests) and split it into `problems.jsonl` and `solutions.jsonl`.

**NOTE:** Do NOT include the generated data in your submission. The `.gitignore` will do this for you.

### Docker Containers

```sh
docker build . -f p5-base.Dockerfile -t p5-base
docker build . -f notebook.Dockerfile -t p5-nb
docker build . -f namenode.Dockerfile -t p5-nn
docker build . -f datanode.Dockerfile -t p5-dn
docker build . -f boss.Dockerfile -t p5-boss
docker build . -f worker.Dockerfile -t p5-worker
```

Note that you need to write a `boss.Dockerfile` and `worker.Dockerfile`. A `build.sh` script is included for your convenience.

You can bring up your cluster like this:

```
export PROJECT=p5
docker compose up -d
```

**Hint:** For the boss and worker dockerfile, look at the lecture code for spark.

### Jupyter Container

Connect to JupyterLab inside your container. Within the `nb` directory, create a notebook called `p5.ipynb`.

Run the following shell commands in a cell to upload the data:

```sh
hdfs dfs -D dfs.replication=1 -cp -f data/*.jsonl hdfs://nn:9000/
hdfs dfs -D dfs.replication=1 -cp -f data/*.csv hdfs://nn:9000/
```

### VS Code users

If you are using VS Code and remote SSH to work on your project, then the ports will already be forwarded for you. And you only need to go to: `http://127.0.0.1:5000/lab` in your terminal.

## Part 1: Filtering: RDDs, DataFrames, and Spark

Inside your `p5.ipynb` notebook, create a Spark session (note we're enabling Hive on HDFS):

```python
from pyspark.sql import SparkSession
spark = (SparkSession.builder.appName("cs544")
         .master("spark://boss:7077")
         .config("spark.executor.memory", "1G")
         .config("spark.sql.warehouse.dir", "hdfs://nn:9000/user/hive/warehouse")
         .enableHiveSupport()
         .getOrCreate())
```

Load `hdfs://nn:9000/problems.jsonl` into a DataFrame. To verify success, run:

```python
problems_df.limit(5).show()
```

If loaded properly, you should see:
![image.png](image.png)

#### Q1: How many problems are there with a `cf_rating` of at least 1600, having `private_tests`, and a name containing "\_A." (Case Sensitive)? Answer by directly using the RDD API.

Remember that if you have a Spark DataFrame `df`, you can get the underlying RDD using `df.rdd`.

**REMEMBER TO INCLUDE `#q1` AT THE TOP OF THIS CELL**

#### Q2: How many problems are there with a `cf_rating` of at least 1600, having `private_tests`, and a name containing "\_A." (Case Sensitive)? Answer by using the DataFrame API.

This is the same question as Q1, and you should get the same answer. This is to give you to interact with Spark different ways.

#### Q3: How many problems are there with a `cf_rating` of at least 1600, having `private_tests`, and a name containing "\_A." (Case Sensitive)? Answer by using Spark SQL.

Before you can use `spark.sql`, write the problem data to a Hive table so that you can refer to it by name.

Again, the result after calling `count` should match your answers for Q1 and Q2.

## Part 2: Hive Data Warehouse

#### Q4: Does the query plan for a GROUP BY on solutions data need to shuffle/exchange rows if the data is pre-bucketed?

Write the data from `solutions.jsonl` to a Hive table named `solutions`, like you did for `problems`. This time, though, bucket the data by "language" and use 4 buckets when writing to the table.

Use Spark SQL to explain the query plan for this query:

```sql
SELECT language, COUNT(*)
FROM solutions
GROUP BY language
```

The `explain` output suffices for your answer. Take note (for your own sake) whether any `Exchange` appears in the output. Think about why an exchange/shuffle is or is not needed between the `partial_count` and `count` aggregates.

<!--
After bucketing the solutions, call `.explain` on a query that counts solutions per language. This should output `== Physical Plan ==` followed by the plan details. You've bucketed correctly if `Bucketed: true` appears in the output.
-->

#### Q5: What tables/views are in our warehouse?

You'll notice additional CSV files in `nb/data` that we haven't used yet. Create a Hive view for each using `createOrReplaceTempView`. Use these files:

```python
[
    "languages", "problem_tests", "sources", "tags"
]
```

Answer with a Python dict like this:

```python
{'problems': False,
 'solutions': False,
 'languages': True,
 'problem_tests': True,
 'sources': True,
 'tags': True}
```

The boolean indicates whether it is a temporary view (True) or table (False).

## Part 3: Caching and Transforming Data

#### Q6: How many correct PYTHON3 solutions are from CODEFORCES?

You may use any method for this question. Join the `solutions` table with the `problems` table using an inner join on the `problem_id` column. Note that the `source` column in `problems` is an integer. Join this column with the `source_id` column from the `sources` CSV. Find the number of correct `PYTHON3` solutions from `CODEFORCES`.

Answer Q6 with code and a single integer. **DO NOT HARDCODE THE CODEFORCES ID**.

#### Q7: How many problems are of easy/medium/hard difficulty?

The `problems_df` has a numeric `difficulty` column. For the purpose of categorizing the problems, interpret this number as follows:

- `<= 5` is `Easy`
- `<= 10` is `Medium`
- Otherwise `Hard`

Your answer should return this dictionary:

```python
{'Easy': 409, 'Medium': 5768, 'Hard': 2396}
```

Note (in case you use this dataset for something beyond the course): the actual meaning of the difficulty column depends on the problem source.

**Hint:** https://www.w3schools.com/sql/sql_case.asp

#### Q8: Does caching make it faster to compute averages over a subset of a bigger dataset?

To test the impact of caching, we are going to do the same calculations with and without caching. Implement a query that first filters rows of `problem_tests` to get rows where `is_generated` is `False` -- use a variable to refer to the resulting DataFrame.

Write some code to compute the average `input_chars` and `output_chars` over this DataFrame. Then, write code to do an experiment as follows:

1. compute the averages
2. make a call to cache the data
3. compute the averages
4. compute the averages
5. uncache the data

Measure the number of seconds it takes each of the three times we do the average calculations.s_generated` filtering only. Answer with list of the three times, in order, as follows:

```
[0.9092552661895752, 1.412867546081543, 0.1958458423614502]
```

Your numbers may vary significantly, but the final run should usually be the fastest.

## Part 4: Machine Learning with Spark

The dataset documentation for the `difficulty` field says: "For Codeforces problems, cf_rating is a more reliable measure of difficulty when available".

For this part, you will attempt to estimate the cf_rating for Codeforces problems for which it is unknown. To prepare, filter the problems to `CODEFORCES` problems, then further divide into three DataFrames:

- train dataset: `cf_rating` is >0, and `problem_id` in an EVEN number
- test dataset: `cf_rating` is >0, and `problem_id` in an ODD number
- missing dataset: `cf_rating` is 0

#### Q9: How well can a decision tree predict `cf_rating` based on `difficulty`, `time_limit`, and `memory_limit_bytes`?

Create a Spark Pipeline model with VectorAssembler and DecisionTreeRegression stages. The max tree depth should be 5. Train it on the training data, then compute an R^2 score (`r2_score`) for predictions on the test data. The R^2 score should be your answer for this question.

#### Q10: Do the problems with a missing `cf_score` appear more or less challenging that other problems?

Use your model to predict the `cf_score` in the dataset where it is missing.

Answer with a tuple with 3 numbers:

- average `cf_rating` in the training dataset
- average `cf_rating` in the test dataset
- average **prediction** of `cf_rating in the missing dataset

For example:

(1887.9377431906614, 1893.1106471816283, 1950.4728638818783)

## Submission

We should be able to run the following on your submission to directly create the mini cluster:

```
docker build . -f p5-base.Dockerfile -t p5-base
docker build . -f notebook.Dockerfile -t p5-nb
docker build . -f namenode.Dockerfile -t p5-nn
docker build . -f datanode.Dockerfile -t p5-dn
docker build . -f boss.Dockerfile -t p5-boss
docker build . -f worker.Dockerfile -t p5-worker

export PROJECT=p5
docker compose up -d
```

We should then be able to open `http://localhost:5000/lab`, find your
notebook, and run it.

## Tester

Coming soon...
