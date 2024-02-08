# Data Engineer Take Home Exercise

## Question
### Data Prep

Write a script to transform input CSV to desired output CSV and Parquet. 

You will find a CSV file in the files folder under `data.csv`. There are three steps to this part of the test. Each step concerns manipulating the values for a single field according to the step's requirements. The steps are as follows:

**String cleaning** - The bio field contains text with arbitrary padding, spacing and line breaks. Normalize these values to a space-delimited string.

**Code swap** - There is a supplementary CSV in the files folder under `state_abbreviations`. This "data dictionary" contains state abbreviations alongside state names. For the state field of the input CSV, replace each state abbreviation with its associated state name from the data dictionary.

**Date offset** - The start_date field contains data in a variety of formats. These may include e.g., "June 23, 1912" or "5/11/1930" (month, day, year). But not all values are valid dates. Invalid dates may include e.g., "June 2018", "3/06" (incomplete dates) or even arbitrary natural language. Add a start_date_description field adjacent to the start_date column to filter invalid date values into. Normalize all valid date values in start_date to ISO 8601 (i.e., YYYY-MM-DD).

Your script should take `data.csv` as input and produce a cleansed `enriched.csv` and `enriched.snappy.parquet` files according to the step requirements above.

## Submission Guidelines
We ask that your solutions be implemented in Python (3.8 or newer) or PySpark (3.3 or newer). If you would like to present skills for both approach, feel free to prepare two separate jupyter notebooks. Assume that code will be used monthly to process the data and store it in AWS S3 based data lake. With that assumption please prepare for discussion how this code can be scheduled and how outputs should be stored in S3 bucket.

### Assessment Criteria
Our goal is not to fool you. On the contrary, we would like to see you in your best light! We value clean, DRY and documented code; and in the interest of full disclosure, our assessment criteria is outlined below (in order of significance):

1. Your ability to effectively solve the problems posed.
2. Your ability to solve these problems in a clear and logical manner, with tasteful design.
3. Your ability to appropriately document and comment your code.


# Project submission - Spark challenge

### Developed by: Jakub Pitera

Files:
- **takehomefile.ipynb** - Main Jupyter notebook with assignment details. It showcases how author created the ETL pipeline with Spark according to the data preperation requirements. 
- **ETL.py** - ETL code reformatted into functions. Can be initialized in terminal.
- **ETL_S3.py** - Alternate version of ETL.py. ETL steps were updated to load data to AWS S3 bucket.
- **dl.cfg** - Template for storing AWS credentials used when writing to S3.
- **airflow_dag_naive.py** - using airflow to run ETL tasks. naive approach. demonstrates usage of airflow 
- **airflow_dag_sparky.py** - using airflow to run spark application. more elegant approach. data is loaded to S3
- **takehomefile_pandas.ipynb** - Alternative Jupyter Notebook with ETL converted to Pandas
- **requirements.txt** - List of required packages
- **data.csv** - raw dataset in CSV format.
- **state_abbreviations.csv** - abbreviations data dictionary in CSV format.
- **enriched.csv** - directory storing final enriched ouput in CSV format
- **enriched.snappy.parquet** - directory storing final enriched output in SNAPPY.PARQUET format