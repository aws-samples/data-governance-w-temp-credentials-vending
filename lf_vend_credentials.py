"""
Fetch temporary credentials provided by LakeFormation to read data from S3
"""
# import libraries
import boto3
import random
import logging
import pandas as pd
from urllib.parse import urlparse
from typing import List, Optional, Dict

# set a logger
logging.basicConfig(
    format="[%(asctime)s] p%(process)s {%(filename)s:%(lineno)d} %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


def _grant_permissions(
    role: str, 
    table_name: str, 
    permissions: List[str], 
    columns: List[str], 
    db_name: str
):
    """
    Authorize fine grained permissions to a given role by performing the following steps:
    1. Create a LF client.
    2. Grant permission to the "role" with fine grained access to the "columns" 
    that are inputs to this function
    """
    try:
        # Create a lakeformation client
        boto3_session = boto3.session.Session()
        lf_client = boto3_session.client("lakeformation")
        # Use the lakeformation client to grant fine grained access to the 
        # "role" with 'SELECT' permissions on "columns"
        lf_client.grant_permissions(
            Principal={"DataLakePrincipalIdentifier": role},
            Resource={
                "TableWithColumns": {
                    "DatabaseName": db_name,
                    "Name": table_name,
                    "ColumnNames": columns,
                }
            },
            Permissions=permissions,
        )
        logger.info(
            f"Fine grained permissions on {role} have been authorized for {columns} columns"
        )
    except Exception as e:
        logger.error(f"Fine grained access could not be granted to {role}: {e}")


def get_lf_temp_credentials(
    role: str,
    db_name: str,
    table_name: str,
    columns: List[str],
    lf_tag_value: str,
    lf_tag_key: str = "LakeFormationAuthorizedCaller",
    permissions: List[str] = ["SELECT"],
    supported_permission_types: List[str] = ["COLUMN_PERMISSION"],
    duration_in_seconds: int = 3600,
) -> Optional[Dict]:
    """
    Get temporary credentials from LakeFormation to read data from a glue data
    catalog table.

    1. Assign FGAC permissions on the data.
    2. Vend temporary credentials
    Returns:
        Dict or None: A dictionary containing temporary vended credentials, 
                      s3_path to the glue data catalog, type of file, for example `csv`, `parquet`, 
                      and the list of columns to get fine grained access to
    """
    try:
        table_info_and_credentials: Optional[Dict] = None
        # grant fine grained permissions to the role for a column list before vending temporary credentials
        # It just grants fine grained access to "role" for "columns" as inputs to this function
        _grant_permissions(role, table_name, permissions, columns, db_name)

        # initialize a test name for the lf session tags (change
        # this test name for your use case - in this function, a random
        # test session name is created everytime this code runs. You can
        # run it with the same session name
        boto3_session = boto3.session.Session()
        account_id: str = boto3.client("sts").get_caller_identity().get("Account")
        random_int: int = random.randint(0, 99)
        lf_session_name: str = account_id + str(random_int)

        # Assume the role with session tags. The session value is configured by the user on the lakeformation console
        lf_client_sts = boto3_session.client("sts")
        resp = lf_client_sts.assume_role(
            RoleArn=role,
            RoleSessionName=lf_session_name,
            Tags=[{"Key": lf_tag_key, "Value": lf_tag_value}],
        )
        assumed_role_session: Optional[boto3.session.Session] = boto3.session.Session(
            aws_access_key_id=resp["Credentials"]["AccessKeyId"],
            aws_secret_access_key=resp["Credentials"]["SecretAccessKey"],
            aws_session_token=resp["Credentials"]["SessionToken"],
        )
        logger.info(f"session with assumed role is created: {assumed_role_session}")

        # This assumed role session is used to vend temporary credentials
        assumed_lf_client = assumed_role_session.client("lakeformation")
        assumed_glue_client = assumed_role_session.client("glue")
        logger.info(
            f"going to give fine grained access to {columns} on {db_name}.{table_name} using {role}"
        )

        # permissions is set to SELECT if input permissions were none
        # SupportedPermissionTypes is set to COLUMN_PERMISSION if input SupportedPermissionTypes were none
        region: str = assumed_role_session.region_name
        # vend the temporary glue table credentials using the assumed lakeformation client on {database}.{table}
        temporary_credential_info = assumed_lf_client.get_temporary_glue_table_credentials(
            TableArn=f"arn:aws:glue:{region}:{account_id}:table/{db_name}/{table_name}",
            Permissions=permissions,
            SupportedPermissionTypes=supported_permission_types,
            DurationSeconds=duration_in_seconds,
        )

        # get unfiltered table metadata on the glue data catalog for role
        # we need this to get the location of s3 path from where to read this data
        # see https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/lakeformation/client/get_temporary_glue_table_credentials.html
        unfiltered_metadata = assumed_glue_client.get_unfiltered_table_metadata(
            CatalogId=account_id,
            DatabaseName=db_name,
            Name=table_name,
            SupportedPermissionTypes=supported_permission_types,
        )
        
        # update the dictionary to contain the temporary glue table credentials, s3_path to the glue table, 
        # file type, and authorized columns to return metdata on.
        table_info_and_credentials = {
            "temporary_credential_info": temporary_credential_info, 
            "s3_path": unfiltered_metadata["Table"]["StorageDescriptor"]["Location"],
            "classification": unfiltered_metadata["Table"]["Parameters"][
                "classification"
            ],
            "columns": unfiltered_metadata["AuthorizedColumns"],
        }
    except Exception as e:
        logger.error(f"Error in read_data_with_lf_policies: {e}")
        table_info_and_credentials: Optional[Dict] = None
    return table_info_and_credentials