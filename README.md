# DeepMind CodeContests Analytics 

A distributed big data pipeline and analytics using Apache Spark, Hadoop File System, and Apache Hive to analyze 8,000+ competitive programming problems from the DeepMind CodeContests dataset.

---

## Key Features

- **Multi-API Analysis**: Implements the same queries using RDD, DataFrame, and Spark SQL APIs
- **Intelligent Bucketing**: Pre-partitions solutions by programming language for optimized joins
- **ML-Powered Predictions**: Predicts CodeForces difficulty ratings using decision trees
- **NL2SQL Interface**: Converts natural language questions to SQL using LLM
- **Performance Analysis**: Evaluates caching impact and model overfitting through experimentation

---


## Dataset

- **Source**: [DeepMind CodeContests](https://huggingface.co/datasets/deepmind/code_contests)
- **Size**: 
  - 8,577 competitive programming problems
  - 100,000+ solutions across multiple languages
  - Metadata: difficulty ratings, time/memory limits, test cases
- **Format**: JSONL files loaded into Hive tables

---
## Architecture

```
┌─────────────┐
│  JupyterLab │ (Port 5000)
└──────┬──────┘
       │
┌──────▼──────────────────────────────┐
│     Spark Cluster                    │
│  ┌──────┐  ┌────────┬────────┐     │
│  │ Boss │──│Worker 1│Worker 2│     │
│  └──────┘  └────────┴────────┘     │
└──────┬──────────────────────────────┘
       │
┌──────▼──────────────────────────────┐
│      Hadoop HDFS                     │
│  ┌─────────┐  ┌──────────────┐     │
│  │NameNode │──│DataNode (x2) │     │
│  └─────────┘  └──────────────┘     │
└──────────────────────────────────────┘
```

**Components:**
- **Spark Boss**: Master node coordinating distributed computations
- **Spark Workers**: Execute parallel tasks across partitions
- **HDFS NameNode**: Manages metadata and file system namespace
- **HDFS DataNodes**: Store actual data blocks with replication

---
## Analysis Highlights

The project answers 10 analytical questions covering:

1. **Query Optimization**: Comparing RDD vs DataFrame vs SQL performance
2. **Join Performance**: Analyzing bucketed vs non-bucketed join strategies
3. **Data Classification**: Categorizing problems by difficulty using CASE statements
4. **Cache Performance**: Measuring 5-10x speedup with intelligent caching
5. **ML Prediction**: Achieving R² > 0.7 for difficulty prediction
6. **Overfitting Analysis**: Visualizing training vs test performance across model depths


---
## 🔍 Sample Queries

**Natural Language Query:**
```python
result = human_query("How many JAVA solutions are there?")
# Returns: 45123
```

**Multi-API Comparison:**
```python
# Same question, three different ways
rdd_count = problems_df.rdd.filter(...).count()
df_count = problems_df.filter(...).count()
sql_count = spark.sql("SELECT COUNT(*) FROM problems WHERE...").collect()[0][0]
```


---
## Technology Stack

- **Data Processing**: Apache Spark 3.x (PySpark)
- **Storage**: Hadoop HDFS 3.x
- **Querying**: Apache Hive, Spark SQL
- **ML**: Spark MLlib (DecisionTreeRegressor, VectorAssembler)
- **LLM**: Google Gemini API (gemini-2.5-flash)
- **Orchestration**: Docker Compose
- **Analysis**: Jupyter Notebook, Pandas, Matplotlib


---

*Built as part of CS 544: Big Data Systems coursework*