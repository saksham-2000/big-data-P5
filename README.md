
## Overview

In P5, you'll use Spark to analyze competitive programming problems and their solutions. You'll load your data into Hive tables and views for easy querying. The main table contains numerous IDs in columns; you'll need to join these with other tables or views to determine what these IDs mean.

**Important:** You'll answer 10 questions in P5. Write each question number and text (e.g., "#q1: ...") as a comment in your notebook before each answer so we can easily find and grade your work.

Learning objectives:

- Interact with Spark via its three programming models
- Optimize Spark queries using caching and bucketing
- Promt an LLM programmatically to generate SQL from a question expressed in natural language
- Use PySpark's machine learning API to train decision trees



For Part 3, you will need to programmically interact with an LLM, and for that you must use the `gemini-2.5-flash` model via the Gemini API (`pip install google-genai==0.2.2`).  Here is an example:

```python
from google import genai
import os

# Configure client with API key from environment variable
client = genai.Client(api_key=os.environ['GEMINI_API_KEY'])

response = client.models.generate_content(
    model='gemini-2.5-flash',
    contents='In one sentence, what is a Spark RDD?',
    config={
        'temperature': 0.7,
    }
)

# Print the result
print(response.text)
```

The above will print "A Spark RDD (Resilient Distributed Dataset) is
an immutable, partitioned collection of data elements distributed
across a cluster and fault-tolerant", or something similar.  The
temperature is not 0, so it will not be deterministic.

The `docker-compose.yml` file we provide will automatically pass the `GEMINI_API_KEY` environment variable from your VM to within your notebook container.

Document your AI usage in `ai.md`.

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

```bash
export GEMINI_API_KEY='your-api-key-here'
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

## Part 1: Spark Programming Models

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

Write the problems data to a Hive table named `problems` so you can query it with Spark SQL.

Load `hdfs://nn:9000/solutions.jsonl` into a DataFrame and write it to a Hive table named `solutions`, bucketing the data by "language" using 4 buckets.

You'll notice additional CSV files in `nb/data` that we haven't used yet. Create a Hive view for each using `createOrReplaceTempView`. Use these files:

```python
[
    "languages", "problem_tests", "sources", "tags"
]
```

#### Q1: How many problems are there with a `cf_rating` of at least 1600, having `private_tests`, and a name containing "\_A." (Case Sensitive)? Answer using all three Spark APIs.

Write code using the RDD API, DataFrame API, and Spark SQL to answer the same question three different ways.

Remember that if you have a Spark DataFrame `df`, you can get the underlying RDD using `df.rdd`.

Return a tuple with three integers: `(rdd_count, dataframe_count, sql_count)`. All three values should be the same.

**REMEMBER TO INCLUDE `#q1` AT THE TOP OF THIS CELL**

#### Q2: How many correct PYTHON3 solutions are from CODEFORCES?

You may use any method for this question. Join the `solutions` table with the `problems` table using an inner join on the `problem_id` column. Note that the `source` column in `problems` is an integer. Join this column with the `source` column from the `sources` CSV. Find the number of correct `PYTHON3` solutions from `CODEFORCES`.

Answer with a single integer. **DO NOT HARDCODE THE CODEFORCES ID**.

#### Q3: How many problems are of easy/medium/hard difficulty?

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

#### Q4: What tables/views are in our warehouse?

Answer with a Python dict indicating whether each table/view is temporary (True) or permanent (False):

```python
{'problems': False,
 'solutions': False,
 'languages': True,
 'problem_tests': True,
 'sources': True,
 'tags': True}
```

## Part 2: Performance Analysis

#### Q5: Does the query plan for a GROUP BY on solutions data need to shuffle/exchange rows if the data is pre-bucketed?

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

#### Q6: Does caching make it faster to compute averages over a subset of a bigger dataset?

To test the impact of caching, we are going to do the same calculations with and without caching. Implement a query that first filters rows of `problem_tests` to get rows where `is_generated` is `False` -- use a variable to refer to the resulting DataFrame.

Write some code to compute the average `input_chars` and `output_chars` over this DataFrame. Then, write code to do an experiment as follows:

1. compute the averages
2. make a call to cache the data
3. compute the averages
4. compute the averages
5. uncache the data

Measure the number of seconds it takes each of the three times we do the average calculations. Answer with list of the three times, in order, as follows:

```
[0.9092552661895752, 1.412867546081543, 0.1958458423614502]
```

Your numbers may vary significantly, but the final run should usually be the fastest.

## Part 3: Spark and Generative AI

In this part, you'll build a natural language interface to your Spark data using the Gemini API. You'll create a function that converts English questions into executable Spark SQL queries that you will automatically run.

Implement the `human_query` function, which takes a string, and returns a single numeric result (as an int):

```python
def human_query(english_question):
    # TODO: your implementation here
```

Use the `google.genai` module as shown earlier.  You will need to write code to carefully construct a prompt for your API call that includes the following:
1. the question that was passed in
2. the schema of your tables/views (you can hardcode the schema or get it programmatically using methods like `spark.sql("DESCRIBE TABLE table_name")` or `spark.catalog.listColumns("table_name")`)
3. directions on how Gemini should format the response so you can programmatically extract it, and pass it to `spark.sql(...)`.  For example, you could instruct Gemini to respond with only the query, and no other content/formatting.  Or you could ask it to respond with markdown formatting, and write the query in a code block like "```...query...```" (in the latter case, you could then write some Python code to extract the query from the code block)

Pass your programmatically constructed prompt to `client.models.generate_content`, extract the generated query from the response, run it with Spark, and return the integer result.  Use temperature 0 to produce deterministic results.  If you get the wrong answer, improve your function to construct a better prompt.

#### Q7: How many JAVA solutions are there?

Use your `human_query` function with the question "How many JAVA solutions are there?" and return the result as an integer.

#### Q8: What is the maximum memory limit in bytes?

Use your `human_query` function with the question "What is the maximum memory limit in bytes?" and return the result as an integer.

## Part 4: Spark and Traditional Machine Learning

The dataset documentation for the `difficulty` field says: "For Codeforces problems, cf_rating is a more reliable measure of difficulty when available".

For this part, you will attempt to estimate the cf_rating for Codeforces problems for which it is unknown. To prepare, filter the problems to `CODEFORCES` problems, then further divide into three DataFrames:

- train dataset: `cf_rating` is >0, and `problem_id` in an EVEN number
- test dataset: `cf_rating` is >0, and `problem_id` in an ODD number
- missing dataset: `cf_rating` is 0

#### Q9: Do the problems with a missing `cf_rating` appear more or less challenging that other problems?

Create a Spark Pipeline model with VectorAssembler and DecisionTreeRegression stages. The max tree depth should be 5. Train it on the training data, using `cf_rating` for the label and the following for the features: `difficulty`, `time_limit` and `memory_limit_bytes`.

Use your model to predict the `cf_rating` in the dataset where it is missing.

Answer with a tuple with 3 numbers:

- average `cf_rating` in the training dataset
- average `cf_rating` in the test dataset
- average **prediction** of `cf_rating` in the missing dataset


#### Q10: How does tree depth impact the quality of predictions?

To better understand model complexity and overfitting, train decision
tree models with varying depths (e.g., 1, 2, 3, 5, 7, 10, 15, 20, 25,
30) on the same training data from Q9. For each depth, evaluate the R^2
score on both the training and test datasets.

Collect the depth and R^2 results from your experiment in a Pandas DataFrame with
these columns: `depth`, `train`, `test`.  It should look like this:

![Overfitting DataFrame](q10df.png)

To communicate your answer, create a plot from the DataFrame that looks like this:

![Overfitting Analysis Example](q10.svg)

Additionally, call `.to_json()` on your plot DataFrame to get a string
representation of the data.

The output for your Q10 cell should be both the plot image and the
JSON string.

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
`p5.ipynb` notebook, and run it.

**Required files in your submission:**
- `boss.Dockerfile` and `worker.Dockerfile`
- `nb/p5.ipynb` - your completed notebook with all 10 questions answered

**Note:** Do NOT include the dataset files (`problems.jsonl`, `solutions.jsonl`, etc.) in your submission. The `.gitignore` file will automatically exclude them.

