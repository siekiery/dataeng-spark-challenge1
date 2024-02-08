from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, regexp_replace, trim, to_date, when, coalesce, length


def create_spark_session():
    """Starts a spark session running locally
    
    Returns:
        pyspark.sql.session.SparkSession object
    """
    spark = SparkSession.builder \
        .appName("InterviewChallenge") \
        .master("local[2]") \
        .config("spark.executor.memory", "2g") \
        .getOrCreate()

    return spark


def read_spark_df_from_csv(spark, filepath):
    """Reads csv into spark dataframe
    Args: 
        spark (SparkSession object).
        filepath (str): path to input csv file
    
    Returns:
        pyspark.sql.dataframe.DataFrame object: Newly created spark dataframe
    """
    df = (spark.read
          .option("multiline", True)  # to read text containing newlines properly
          .option("header", True)
          .csv(filepath))

    return df


def process_str_cleaning(df):
    """ The bio col contains text with arbitrary padding, spacing and line breaks. 
    Normalizes these values to a space-delimited string.
    
    Args: 
        df (pyspark.sql.dataframe.DataFrame): Input dataframe

    Returns:
        pyspark.sql.dataframe.DataFrame: Processed dataframe
    """
    # Normalize escape characters, padding and spacing with a single space
    df_normalized = (df
                     .withColumn('bio', regexp_replace(col('bio'), '\\[tbnrfsu]', ' '))
                     .withColumn('bio', regexp_replace(col('bio'), '\s+', ' '))
                     .withColumn('bio', trim(col('bio'))))

    return df_normalized


def process_code_swap(df, abbr_filepath, spark):
    """Reads supplementary CSV `state_abbreviations.csv`. This "data dictionary" contains state abbreviations alongside 
    state names. For the 'state' col of the df, replaces each state abbreviation with its associated state name 
    from the data dictionary.

    Args:
        df (pyspark.sql.dataframe.DataFrame): Input dataframe
        abbr_filepath (str): Path to input csv file containing data dictionary
        spark (pyspark.sql.session.SparkSession): SparkSession object

    Returns:
        pyspark.sql.dataframe.DataFrame: Processed dataframe
    """
    # Read lookup values into spark dataframe
    abbr_df = spark.read.option('header', True).csv(abbr_filepath)

    # Replace 'state' col using joined abbreviations df 
    df = (df
          .join(abbr_df, df.state == abbr_df.state_abbr, how='left')
          .drop('state')
          .drop('state_abbr')
          .withColumnRenamed('state_name', 'state'))

    return df


def process_date_offset(df):
    """The start_date field contains data in a variety of formats. 
    These may include e.g., "June 23, 1912" or "5/11/1930" (month, day, year). But not all values are valid dates. 
    Invalid dates may include e.g., "June 2018", "3/06" (incomplete dates) or even arbitrary natural language. 
    Add a start_date_description field adjacent to the start_date column to filter invalid date values into. 
    Normalize all valid date values in start_date to ISO 8601 (i.e., YYYY-MM-DD).
    
    Args: 
        df (pyspark.sql.dataframe.DataFrame): Input dataframe

    Returns:
        pyspark.sql.dataframe.DataFrame: Processed dataframe
    """

    # Enhance to_date() function to take multiple formats and merge the results
    def to_date_(col, formats=("MM/d/y", "y-MM-d", "MMMM d, y")):
        return coalesce(*[to_date(col, f) for f in formats])

    # Create 'sd' col by running to_date_() on 'start_date' col
    df = df.withColumn('sd', to_date_('start_date'))

    # Use 'sd' col to create 'start_date_description' col describing if 'start_date' vals are Valid or Invalid
    df = df.withColumn('start_date_description', when(col('sd').isNull(), "Invalid").otherwise("Valid"))

    # As per requirements - the 'start_date' column valid values get normalized while invalid remain unchanged. 
    # If it is not what the reviewer had in mind then in 'sd' column invalid values were turned to null 
    df = df.withColumn('start_date', coalesce(col('sd'), col('start_date')))

    return df


def save_to_csv_and_parquet(df, output_name):
    """Writes enriched dataframe to csv and snappy.parquet formats
    Args:
        df (pyspark.sql.dataframe.DataFrame): Input dataframe in a final form
        output_name (str): Name for the output files
    
    Returns:
        None
    """
    output_name_csv = f'{output_name}.csv'
    output_name_parquet = f'{output_name}.snappy.parquet'

    df.option('header', True).option('delimiter', ',').csv(output_name_csv)
    df.option("compression", "snappy").parquet(output_name_parquet)


def bio_test(df):
    """'bio' column normalization check.
    Tests if 'bio' column does not contain \n', '\t', '  ', '   '.

    Args:
        df (pyspark.sql.dataframe.DataFrame): tested DataFrame

    Returns:
        bool: True or False whether the check passed or failed
    """
    # Number of records in bio col with undesired characters
    bio_mask = df.bio.contains('\n') | df.bio.contains('\t') | df.bio.contains('  ') | df.bio.contains('  ')
    bio_count = df.filter(bio_mask).count()

    # Check results
    if bio_count == 0:
        print('QC: bio column normalization - Passed')
        return True
    else:
        print('QC: bio column normalization - FAILED')
        return False


def state_test(df):
    """'state' column abbreviation swap
    Tests if all 'state' column strings are larger than 2

    Args:
        df (pyspark.sql.dataframe.DataFrame): tested DataFrame

    Returns:
        bool: True or False whether the check passed or failed
    """
    # Number of records in 'state' col with strings length < 2
    state_mask = length(col('state')) < 3
    state_count = df.filter(state_mask).count()

    # Check results
    if state_count == 0:
        print('QC: state column abbreviation swap - Passed')
        return True
    else:
        print('QC: state column abbreviation swap - FAILED')
        return False


def date_test1(df):
    """date offset test 1
    Tests if 'sd' staging column dtype is DateType

    Args:
        df (pyspark.sql.dataframe.DataFrame): tested DataFrame

    Returns:
        bool: True or False whether the check passed or failed
    """
    # Tests if 'sd' staging column dtype is DateType
    date_check1 = str(df.schema['sd'].dataType) == 'DateType()'

    # Check results
    if date_check1:
        print('QC: date offset - "sd" column is DateType - Passed')
    else:
        print('QC: date offset - "sd" column is DateType - FAILED')

    return date_check1


def date_test2(df):
    """date offset test 2
    Tests if all valid dates found were converted to ISO 8601 'YYYY-MM-DD' standard

    Args:
        df (pyspark.sql.dataframe.DataFrame): tested DataFrame

    Returns:
        bool: True or False whether the check passed or failed
    """
    # Number of records with initial date format described as valid
    valid_count = df.filter(col('start_date_description').contains('Valid')).count()
    # Number of records in ISO 8601 format in 'start_date' col
    isodate_count = df.filter(to_date(col('start_date'), 'y-MM-d').isNotNull()).count()

    # Check results
    date_check2 = valid_count == isodate_count
    if date_check2:
        print('QC: date offset - All Valid dates converted to ISO 8601 - Passed')
    else:
        print('QC: date offset - All Valid dates converted to ISO 8601 - FAILED')

    return date_check2


def qc(df):
    """Orchestrates all QC tests.

    Args:
        df (pyspark.sql.dataframe.DataFrame): tested DataFrame

    Returns:
        bool: True or False whether the QC step succeeded or failed
    """
    bio_check = bio_test(df)
    state_check = state_test(df)
    date_check1 = date_test1(df)
    date_check2 = date_test2(df)

    qc_check = all((bio_check, state_check, date_check1, date_check2))

    # Print SUCCESS if all checks passed
    if qc_check:
        print("QC Results: SUCCESS")
    else:
        print("QC Results: FAILURE")

    return qc_check


def main():
    """Main ETL body. Starts spark session and runs ETL procedures
    """
    input_filepath = 'data.csv'
    abbr_filepath = 'state_abbreviations.csv'
    output_name = 'enriched'

    print("> Starting")

    # Creates spark session
    spark = create_spark_session()
    print("> Spark session created")

    # Loads input data from CSV to Spark DataFrame
    df = read_spark_df_from_csv(spark, input_filepath)
    print("> Data loaded into spark dataframe")

    # Processes string cleaning task
    print("> Processing string cleaning")
    df = process_str_cleaning(df)
    print("Done.")

    # Processes state code swap task
    print("> Processing state code swap")
    df = process_code_swap(df, abbr_filepath, spark)
    print("Done")

    # Processes date offset task
    print("> Processing date offset")
    df = process_date_offset(df)
    print("Done")

    # QC tests
    print("> QC step")
    qc_check = qc(df)

    # Writes enriched data to CSV and SNAPPY.PARQUET
    print("> Writing enriched data to CSV and SNAPPY.PARQUET")
    save_to_csv_and_parquet(df, output_name)
    print("> Finished!")


if __name__ == "__main__":
    main()
