## Logging Config

#### Set up logging config

```python
import logging
logging.basicConfig(format="%(asctime)s %(message)s",
                    datefmt="%m/%d/%Y %I:%M:%S %p",
                    level=logging.INFO)
```

#### Filter python module

By setting the module logging level as `logging.ERROR`.

```python
import logging
logging.getLogger("py4j").setLevel(logging.ERROR)
```

## Reduce Operations

```python
from functools import reduce
from operator import and_, or_, add
add_all = lambda *not_null_vals: reduce(add, not_null_vals)
and_all = lambda *logical_vars: reduce(and_, logical_vars)
or_all = lambda *logical_vars: reduce(or_, logical_vars)
```

* Be careful of `add`. If there is `None`, the return will be `None`.

## Pyspark

#### Union all DataFrames with the same schema

```python
from functools import reduce
from pyspark.sql import DataFrame
union_all = lambda *dataframe_lists: reduce(DataFrame.unionByName,
                                            dataframe_lists)
```

#### Apply udf with High-Order function

```python
import pyspark.sql.functions as F

def define_lt_udf(argument_list, game):
    return "in_list" if game in argument_list else "out_of_list"
  
def define_lt(argument_list):
    return F.udf(lambda game: define_lt_udf(argument_list, game))

argument_list = ["args1", "args2"]
df = df.withColumn("if_in_list", define_lt(argument_list)(F.col("game")))
```

#### Apply udf on row level

```python
import pyspark.sql.functions as F
import pyspark.sql.types as T

_udf = F.udf( lambda _row: func(_row), T.DoubleType())
df = df.withColumn("new_col",
                   _udf(F.struct([song_df[_col] for _col in df.columns])))
```

#### Pivot and Unpivot

wide table:

CAT | A | B | C
-----|-----|---|--
cat1 |a1 | b1 | c1
cat2 |a2 | b2 | c3

long table:

CAT | key | val
-----|-----|--
cat1 | A | a1 
cat2 | A | a2 
cat1 | B | b1 
cat2 | B | b2 
cat1 | C | c1 
cat2 | C | c2 

```python
# wide -> long
import pyspark.sql.functions as F

pivot_cols = ["CAT"]
sub_strs = []

for _col in ["A", "B", "C"]:
  # "value of column key, value of column val" in long table
  sub_str =  "'{}', {}".format(_col, _col)
  sub_strs.append(sub_str)

exprs = ("stack({}, ".format(len(sub_strs)) +
         ", ".join(sub_strs) +
         ") as (key, val)")
long_table = wide_table.select(*pivot_cols, F.expr(exprs))
# exprs = "stack(3, 'A', A, 'B', B, 'C', C) as (key, val)"
```

```python
# long -> wide
wide_table = long_table.groupBy(pivot_cols).pivot('key').agg({'val':'sum'})
```

#### Databricks Merge into

```python
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
import logging
logging.getLogger("py4j").setLevel(logging.ERROR)
logging.basicConfig(format="%(asctime)s %(message)s",
                    datefmt="%m/%d/%Y %I:%M:%S %p", level=logging.INFO)

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext
sqlContext = SQLContext(sc)


def update_by_merge(source, update, match_on, update_sets=None, if_run=True,
                    update_match=True, insert_not_match=True,
                    only_update_match_null=False, show_stats=True):
    """To use MERGE INTO functionality of DataBricks.
    Attention: Make sure all match_on columns have no null value.
    Otherwise, it will insert new rows instead of update existing rows.

    Args:
        source (str): table name of target data
        update (pyspark.DataFrame): new data
        match_on (list(str)): list of columns used to match
        update_sets ([str]): list of columns need to update when matched,
                                when None, update all columns
        if_run (boolean): if run the merge clause or just return the str
        update_match (boolean): if update matched rows
        insert_not_match (boolean): if insert new rows when not matched
        only_update_match_null (boolean): if only update null value when the row is matched,
                                            when True, must specify update_sets
        show_stats (boolean): if show basic stats of the operation

    Return:
        clause (str): merge into clause
    """

    assert isinstance(match_on, list), "match_on needs to be a list"
    assert isinstance(update_sets, list) or update_sets is None, \
        "update_sets needs to be a list or None"
    assert not (update_match and only_update_match_null and update_sets is None), \
        "if only update null value when the row is matched, must specify update_sets"
    assert not (not update_match and only_update_match_null), \
        "to only update null value when the row is matched, must set update_match as True"

    original_source_n = spark.table(source).count() if (show_stats and if_run) else None

    update_table = source.split(".")[-1]+"_updates"
    update.createOrReplaceTempView(update_table)
    match_clause = (" and ".join(["".join([source, ".", col, " = ", update_table, ".", col])
                                  for col in match_on]))
    update_clause = (", ".join(["".join([col, " = ", update_table, ".", col])
                                for col in update_sets])
                     if update_sets else "*")
    update_null_clause = (", ".join(["{col}=coalesce({source}.{col},{update}.{col})".format(col=col,
                                                                                            source=source,
                                                                                            update=update_table)
                                     for col in update_sets])
                          if update_sets else "*")
    part_clause = ["MERGE INTO", source, "USING", update_table,
                   "\n ON", match_clause]
    if update_match:
        if only_update_match_null:
            part_clause.extend(["\nWHEN MATCHED THEN \n UPDATE SET", update_null_clause])
        else:
            part_clause.extend(["\nWHEN MATCHED THEN \n UPDATE SET", update_clause])

    if insert_not_match:
        part_clause.append("\nWHEN NOT MATCHED THEN INSERT *")
    clause = " ".join(part_clause)

    if if_run:
        sqlContext.sql(clause)

    if (show_stats and if_run):
        total_update_n = update.count()
        merged_source_n = spark.table(source).count()
        new_row_n = merged_source_n - original_source_n
        updated_row_n = (total_update_n - new_row_n if insert_not_match
                         else "due to the un-insert not-match row, not sure how many")
        logging.info("{new_row_n} rows are inserted, {updated_row_n} rows "
                     "are updated (even by same value)".format(new_row_n=new_row_n,
                                                               updated_row_n=updated_row_n))
    return clause
```

## Read from google sheet

```python
import pandas as pd
import numpy as np
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
import logging
logging.getLogger("google.oauth2").setLevel(logging.ERROR)
logging.getLogger("googleapiclient").setLevel(logging.ERROR)

def google_auth(service_name="sheets", api_version="v4"):
    """Create a google service.
    """

    cred = Credentials(client_id,
                       client_secret,
                       refresh_token,
                       token),
                       token_uri="https://www.googleapis.com/oauth2/v4/token")

    return build(service_name, api_version, credentials=cred, cache_discovery=False)


def read_google_data(service, spreadsheet_id, range_name="Sheet1!A:Z", first_row_as_col_name=True):
    """Make sure the Google Account has access to the workbook.
    Attention: the api can not handle the missing value on sheet well.

    Args:
        service (object):
        spreadsheet_id (str): the id of google shit, normally included in the URL
        range_name (str): same format as normal sheet "sheet_name!range"
        first_row_as_col_name (boolean): default is True

    Return:
        res (pd.DataFrame): pandas dataframe
    """

    result = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id,
                                                 range=range_name).execute()
    if first_row_as_col_name:
        # check longest data length
        data_col_len = max([len(x) for x in result["values"][1::]])
        col_len = len(result["values"][0])
        if data_col_len == col_len:
            res = pd.DataFrame(result["values"][1::], columns=result["values"][0])
        else:
            res = pd.DataFrame(result["values"][1::], columns=result["values"][0][0:data_col_len])
            res = res.reindex(columns=result["values"][0])
    else:
        res = pd.DataFrame(result["values"])
    return res
```

## Tableau API

#### Refresh Data

```python
import requests
import tableauserverclient as TSC
import xml.etree.ElementTree as ET
import logging
logging.getLogger("py4j").setLevel(logging.ERROR)
logging.basicConfig(format="%(asctime)s %(message)s",
                    datefmt="%m/%d/%Y %I:%M:%S %p", level=logging.INFO)

tableau_server = "https://prod-useast-a.online.tableau.com"
tableau_site_id = "tiltingpoint"
tableau_xmlns = {"t": "http://tableau.com/api"}
tableau_username = None
tableau_pw = None

def get_tableau_server():
    tableau_auth = TSC.TableauAuth(tableau_username, tableau_pw, site_id=tableau_site_id)
    server = TSC.Server(tableau_server)
    server.version = "3.0"
    server.auth.sign_in(tableau_auth)
    return server


def tableau_refresh(workbook_id):
    """Refresh tableau workbook.

    Args:
        workbook_id (str): the id of workbook, it can be retrieved by titlingpoint.tableau_get_wb_id()
    """

    server = get_tableau_server()
    # refresh extrect
    try:
        server.workbooks.populate_connections
        server.workbooks.refresh(workbook_id)
        logging.info("Refresh Success")
    except Exception as e:
        logging.error(e)
        raise e


def refresh_datasource(datasource_id, auth_token, site_id):
    """Refreshes datasource using the datasource_id
    """

    VERSION = "3.4"

    url = tableau_server + "/api/{0}/sites/{1}/datasources/{2}/refresh".format(VERSION, site_id, datasource_id)
    xml_request = "<tsRequest></tsRequest>"
    server_response = requests.post(url, data=xml_request,
                                    headers={"x-tableau-auth": auth_token, "Content-Type": "application/xml"})
    server_response = server_response.text.encode("ascii", errors="backslashreplace").decode("utf-8")
    parsed_response = ET.fromstring(server_response)
    logging.info(datasource_id + ": Success!")


def refresh_tableau_datasource(datasources=[]):
    """Use direct APIs instead of the python binding through Tableau Server Client

    Args:
        datasources ([object]): list of datasource objects
    """

    if isinstance(datasources, list):
        # sign into Tableau Online
        auth_token, site_id = sign_in(tableau_username, tableau_pw)

        completed = []
        failed = []
        total_tables = len(datasources)
        for datasource in datasources:
            try:
                refresh_datasource(datasource.id, auth_token, site_id)
                completed.append(datasource.name)
                logging.info("Refreshing: {} successful! ID: {} from Project {}".format(datasource.name,
                                                                                        datasource.id,
                                                                                        datasource.project_name))
                logging.info("{0}/{1} successful".format(len(completed), total_tables))
            except IndexError as e:
                failed.append(datasource.name)
                logging.info("Refreshing: {} failed! ID: {} from Project {}".format(datasource.name,
                                                                                    datasource.id,
                                                                                    datasource.project_name))
                logging.info("{0}/{1} failed".format(len(completed), total_tables))
        sign_out(auth_token)
    else:
        logging.error("This function requires a list of datasource ids to work")


def _check_status(server_response, success_code):
    """
    Checks the server response for possible errors.
    "server_response"       the response received from the server
    "success_code"          the expected success code for the response
    Throws an ApiCallError exception if the API call fails.
    """
    if server_response.status_code != success_code:
        parsed_response = ET.fromstring(server_response.text)

        # Obtain the 3 xml tags from the response: error, summary, and detail tags
        error_element = parsed_response.find("t:error", namespaces=tableau_xmlns)
        summary_element = parsed_response.find(".//t:summary", namespaces=tableau_xmlns)
        detail_element = parsed_response.find(".//t:detail", namespaces=tableau_xmlns)

        # Retrieve the error code, summary, and detail if the response contains them
        code = error_element.get("code", "unknown") if error_element is not None else "unknown code"
        summary = summary_element.text if summary_element is not None else "unknown summary"
        detail = detail_element.text if detail_element is not None else "unknown detail"
        error_message = "{0}: {1} - {2}".format(code, summary, detail)
        raise ApiCallError(error_message)
    return


def sign_in(username, password):
    """Signs in to the tableau server
    """

    VERSION = "3.4"

    logging.info("Signing into Tableau...")
    url = tableau_server + "/api/{0}/auth/signin".format(VERSION)

    xml_request = ET.Element("tsRequest")
    credentials_element = ET.SubElement(xml_request, "credentials", name=username, password=password)
    ET.SubElement(credentials_element, "site", contentUrl=tableau_site_id)
    xml_request = ET.tostring(xml_request)

    server_response = requests.post(url, data=xml_request)

    _check_status(server_response, 200)

    server_response = server_response.text.encode("ascii", errors="backslashreplace").decode("utf-8")
    parsed_response = ET.fromstring(server_response)

    token = parsed_response.find("t:credentials", namespaces=tableau_xmlns).get("token")
    site_id = parsed_response.find(".//t:site", namespaces=tableau_xmlns).get("id")

    logging.info("Signed in!")
    return token, site_id


def sign_out(auth_token):
    """Destroys the active session and invalidates authentication token.
    """

    VERSION = "3.4"
    url = tableau_server + "/api/{0}/auth/signout".format(VERSION)
    server_response = requests.post(url, headers={"x-tableau-auth": auth_token})
    _check_status(server_response, 204)
    logging.info("Signed out!")
    return
```

#### Retrieve Information

```python
import tableauserverclient as TSC

tableau_server = "https://prod-useast-a.online.tableau.com"
tableau_site_id = "tiltingpoint"
tableau_xmlns = {"t": "http://tableau.com/api"}
tableau_username = None
tableau_pw = None

def get_tableau_server():
    tableau_auth = TSC.TableauAuth(tableau_username, tableau_pw, site_id=tableau_site_id)
    server = TSC.Server(tableau_server)
    server.version = "3.0"
    server.auth.sign_in(tableau_auth)
    return server

def tableau_get_wb_id(workbook_full_name):
    """Get the workbook id by workbook full name

    Args:
        workbook_full_name (str): full name of workbook shown on tableau
    """

    server = get_tableau_server()
    workbook_id = [wb.id for wb in TSC.Pager(server.workbooks) if wb.name.startswith(workbook_full_name)]
    return workbook_id


def get_datasource_from_name(name):
    """Extracts datasources using a specified datasource name on Tableau Online
    Returns the datasource objects

    Args:
        name (str): full name of data source shown on tableau
    """

    server = get_tableau_server()
    all_datasources, pagination_item = server.datasources.get()
    data_source = [datasource for datasource in all_datasources if datasource.name.startswith(name)]
    return data_source


def get_datasource_id(table_name):
    """Extracts datasources id using a specified datasource name on Tableau Online
    Returns the datasource ids

    Args:
        table_name (str): full name of data source shown on tableau
    """

    server = get_tableau_server()
    all_datasources, pagination_item = server.datasources.get()
    data_source_id = [datasource.id for datasource in all_datasources if datasource.name.startswith(table_name)]
    return data_source_id
```