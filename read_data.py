"""
Read data from S3 using temporary vended credentials in a pandas/spark dataframe
"""
# import libraries
import boto3
import logging
import pyspark
import pandas as pd
from pyspark import SparkConf
from urllib.parse import urlparse
from pyspark.sql import SparkSession
from typing import List, Optional, Dict

# set a logger
logging.basicConfig(
    format="[%(asctime)s] p%(process)s {%(filename)s:%(lineno)d} %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


def read_pandas_lf_data(
    table_info_and_credentials: Dict,
) -> Optional[pd.core.frame.DataFrame]:
    
    """
    Reads data from the S3 path using temporary vended credentials and subsets based on allowed
    column access in a pandas dataframe
    """
    
    df: Optional[pd.core.frame.DataFrame] = None
    # Initialize a df list that will contain the subsetted data to be returned
    df_list: List[pd.core.frame.DataFrame] = []
    try:
        # Read the CSV file from S3, add another elif for parquet, etc.
        if table_info_and_credentials.get("classification") == "csv":
            s3r = boto3.resource("s3")
            data_file_uri: Optional[str] = None
            # "s3_path" format: s3://{bucket_name}/{database_name}/{table_name}
            # Tested example: s3://sagemaker-lf-trial-bucket2041/cred_vending_test_db/cred_vending_test_table
            s3_path: str = table_info_and_credentials["s3_path"]
            columns: List[str] = table_info_and_credentials["columns"]
            temporary_credential_info: Dict = (
                table_info_and_credentials.get(
                    "temporary_credential_info"
                )
            )
            o = urlparse(s3_path, allow_fragments=False)
            bucket_name = o.netloc
            # Iterate through objects in the bucket
            all_files = list(
                s3r.Bucket(bucket_name).objects.filter(Prefix=f"{o.path[1:]}/").all()
            )
            for file_info in all_files:
                key = file_info.key
                if key.endswith(".csv"):
                    data_file = key.split("/")[-1]
                    data_file_uri = f"{s3_path}/{data_file}"
                    logger.info(f"going to read data from {data_file_uri}")
                    # read data with pandas using temp credentials
                    df = pd.read_csv(
                        data_file_uri,
                        storage_options={
                            "key": temporary_credential_info["AccessKeyId"],
                            "secret": temporary_credential_info["SecretAccessKey"],
                            "token": temporary_credential_info["SessionToken"],
                        },
                    )
                    logger.info(
                        f"_extract_subset_data, shape of data before subsetting={df.shape}"
                    )
                    # append the subsetted df into the df list that is returned
                    subsetted_df = df[columns]
                    logger.info(
                        f"_extract_subset_data, shape of data after subsetting={subsetted_df.shape}"
                    )
                    df_list.append(subsetted_df)
            df = pd.concat(df_list)
            logger.info(
                f"_extract_subset_data, shape of data being returned after concatenating all files={df.shape}"
            )
        else:
            logger.error(f"unsupported file_type={file_type}")
    except Exception as e:
        logger.error(f"columns cannot be queried from s3: {e}")
        df: Optional[pd.core.frame.DataFrame] = None
    return df


def read_spark_lf_data(
    table_info_and_credentials: Dict,
    use_s3a: bool,
) -> Optional[pyspark.sql.dataframe.DataFrame]:
    """
    Reads data from the S3 path using temporary vended credentials and subsets based on allowed
    column access in a spark dataframe
    """
    df: Optional[pyspark.sql.dataframe.DataFrame] = None
    # Initialize a df list that will contain the subsetted data to be returned
    subsetted_df: Optional[pyspark.sql.dataframe.DataFrame] = None
    # "s3_path" format: s3://{bucket_name}/{database_name}/{table_name}
    # Tested example: s3://sagemaker-lf-trial-bucket2041/cred_vending_test_db/cred_vending_test_table
    s3_path: str = table_info_and_credentials["s3_path"]
    if use_s3a is True:
        s3_path = s3_path.replace('s3://', 's3a://') 
    columns: List[str] = table_info_and_credentials["columns"]
    temporary_credential_info: Dict = table_info_and_credentials.get(
        "temporary_credential_info"
    )
    # initialize the spark configuration
    conf = SparkConf()
    # if credentials are not provided, an error occurs: No AWS Credentials provided by TemporaryAWSCredentialsProvider :
    # org.apache.hadoop.fs.s3a.CredentialInitializationException: Access key, secret key or session token is unset
    conf.setAll(
        [
            ("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.2"),
            (
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider",
            ),
            (
                "spark.hadoop.fs.s3a.access.key",
                temporary_credential_info["AccessKeyId"],
            ),
            (
                "spark.hadoop.fs.s3a.secret.key",
                temporary_credential_info["SecretAccessKey"],
            ),
            (
                "spark.hadoop.fs.s3a.session.token",
                temporary_credential_info["SessionToken"],
            ),
        ]
    )
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    logger.info(f"spark session created.")
    try:
        # Read the CSV file from S3, add another elif for parquet, etc.
        if table_info_and_credentials["classification"] == "csv":
            # if the s3a path is not required, replace the variable name below with `s3_path`
            df = spark.read.csv(s3_path, header=True) 
            subsetted_df = df.select(columns)
            subsetted_df = subsetted_df.show(vertical = True)
        else:
            logger.error(f"unsupported file_type={file_type}")
    except Exception as e:
        logger.error(f"columns cannot be queried from s3: {e}")
        subsetted_df: Optional[pd.core.frame.DataFrame] = None
    return subsetted_df
