import time
# from databricks.connect import DatabricksSession
# from databricks.sdk import WorkspaceClient
import os
import zipfile
import re
import argparse
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql import functions as F
# spark = SparkSession.builder.appName("Tradepanel").getOrCreate()

# Define the light refined path
LIGHT_REFINED_PATH='tp-publish-data/'

import argparse
def read_run_params():
    parser = argparse.ArgumentParser()
    parser.add_argument("--FILE_NAME",type=str)
    parser.add_argument("--CNTRT_ID",type=str)
    parser.add_argument("--RUN_ID",type=str)
    args = parser.parse_args()
    return args

def get_dbutils(spark):
    try:
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark)
    except ImportError:
        import IPython
        ipy = IPython.get_ipython()
        if ipy:
            dbutils = ipy.user_ns["dbutils"]
    return dbutils

# spark = SparkSession.builder.appName("Tradepanel").getOrCreate()
# spark = DatabricksSession.builder.getOrCreate()
# dbutils = get_dbutils(spark)

# # Retrieve secrets from Databricks
# refDBjdbcURL = dbutils.secrets.get('tp_dpf2cdl', 'refDBjdbcURL')
# refDBname = dbutils.secrets.get('tp_dpf2cdl', 'refDBname')
# refDBuser = dbutils.secrets.get('tp_dpf2cdl', 'refDBuser')
# refDBpwd = dbutils.secrets.get('tp_dpf2cdl', 'refDBpwd')
# catalog_name = dbutils.secrets.get('tp_dpf2cdl', 'databricks-catalog-name')
# postgres_schema = dbutils.secrets.get('tp_dpf2cdl', 'database-postgres-schema')

# w = WorkspaceClient()

# refDBjdbcURL = w.dbutils.secrets.get('tp_dpf2cdl', 'refDBjdbcURL')
# refDBname = w.dbutils.secrets.get('tp_dpf2cdl', 'refDBname')
# refDBuser = w.dbutils.secrets.get('tp_dpf2cdl', 'refDBuser')
# refDBpwd = w.dbutils.secrets.get('tp_dpf2cdl', 'refDBpwd')
# catalog_name = w.dbutils.secrets.get('tp_dpf2cdl', 'databricks-catalog-name')
# postgres_schema = w.dbutils.secrets.get('tp_dpf2cdl', 'database-postgres-schema')


# Function to initialize Spark and secrets (only when needed)
# def initialize_environment():
#     spark = SparkSession.builder.appName("Tradepanel").getOrCreate()
#     dbutils = get_dbutils(spark)

#     # Retrieve secrets from Databricks
#     refDBjdbcURL = dbutils.secrets.get('tp_dpf2cdl', 'refDBjdbcURL')
#     refDBname = dbutils.secrets.get('tp_dpf2cdl', 'refDBname')
#     refDBuser = dbutils.secrets.get('tp_dpf2cdl', 'refDBuser')
#     refDBpwd = dbutils.secrets.get('tp_dpf2cdl', 'refDBpwd')
#     catalog_name = dbutils.secrets.get('tp_dpf2cdl', 'databricks-catalog-name')
#     postgres_schema = dbutils.secrets.get('tp_dpf2cdl', 'database-postgres-schema')
    
#     return {
#         "spark": spark,
#         "dbutils": dbutils,
#         "refDBjdbcURL": refDBjdbcURL,
#         "refDBname": refDBname,
#         "refDBuser": refDBuser,
#         "refDBpwd": refDBpwd,
#         "catalog_name": catalog_name,
#         "postgres_schema": postgres_schema
#     }

# Only run this block when executing the script directly
if __name__ == "__main__":
    print("Environment initialized successfully.")

###############################################################################

def load_cntrt_col_assign(cntrt_id, postgres_schema, spark, refDBjdbcURL, refDBname, refDBuser, refDBpwd ):
    query = f'''select * from {postgres_schema}.mm_col_asign_lkp where cntrt_id = {cntrt_id}'''
    # Read the data from PostgreSQL into a DataFrame
    df_dpf_col_asign_vw=read_query_from_postgres(query, spark, refDBjdbcURL, refDBname, refDBuser, refDBpwd )
    return df_dpf_col_asign_vw

def load_cntrt_lkp(cntrt_id, postgres_schema, spark, refDBjdbcURL, refDBname, refDBuser, refDBpwd ):
    query = f'''SELECT * FROM {postgres_schema}.mm_cntrt_lkp WHERE cntrt_id= {cntrt_id}'''
    # Read the data from PostgreSQL into a DataFrame
    df_cntrt_lkp=read_query_from_postgres(query, spark, refDBjdbcURL, refDBname, refDBuser, refDBpwd )
    return df_cntrt_lkp

def load_cntrt_file_lkp(cntrt_id, dmnsn_name, postgres_schema, spark, refDBjdbcURL, refDBname, refDBuser, refDBpwd ):
    query = f"""SELECT file_patrn FROM {postgres_schema}.mm_cntrt_file_lkp WHERE cntrt_id={cntrt_id} AND dmnsn_name='{dmnsn_name}' """
    # Read the data from PostgreSQL into a DataFrame
    df_cntrt_file_lkp=read_query_from_postgres(query, spark, refDBjdbcURL, refDBname, refDBuser, refDBpwd )
    return df_cntrt_file_lkp

def load_cntrt_dlmtr_lkp(cntrt_id, dmnsn_name, postgres_schema, spark, refDBjdbcURL, refDBname, refDBuser, refDBpwd ):
    query = f"""SELECT dt.dlmtr_val FROM {postgres_schema}.mm_cntrt_file_lkp ct join {postgres_schema}.mm_col_dlmtr_lkp dt on ct.dlmtr_id = dt.dlmtr_id WHERE cntrt_id={cntrt_id} AND dmnsn_name='{dmnsn_name}' """
    # Read the data from PostgreSQL into a DataFrame
    df_cntrt_dlmtr_lkp=read_query_from_postgres(query, spark, refDBjdbcURL, refDBname, refDBuser, refDBpwd )
    return df_cntrt_dlmtr_lkp
def acn_prod_trans_materialize(df,run_id):
    path=f'/mnt/{LIGHT_REFINED_PATH}ACN_Prod_Load/{run_id}/ACN_Prod_Load_chain/tp_mm_ACN_Prod_Load_chain.parquet'
    df.coalesce(1).write.format("parquet").options(header=True).mode('overwrite').save(path)
def acn_prod_trans(srce_sys_id, run_id, catalog_name , spark):
    # Read PROD_DIM schema from Delta Table
    df_sch_prod_dim = spark.sql(f"SELECT * FROM {catalog_name}.gold_tp.tp_prod_dim limit 0")

    # Create a temporary view from the complemented DataFrame
    df_sch_prod_dim.createOrReplaceTempView("df_sch_prod_dim")
    query = f""" select a.*except(a.run_id,a.prod_skid,a.cntrt_id, a.srce_sys_id, a.part_srce_sys_id) from df_sch_prod_dim a"""
    df_sch_prod_dim = spark.sql(query)
    # Read the Product parquet file into DataFrame
    df_rawfile_input = spark.read.parquet(f"/mnt/tp-source-data/temp/materialised/{run_id}/load_product_df_prod_extrn")
    print("Product row count from Raw file: "+str(df_rawfile_input.count())+"\n\n")

    # Complement the columns of df_rawfile_input with those in tp_prod_dim
    df_rawfile_input = df_rawfile_input.unionByName(df_sch_prod_dim,allowMissingColumns=True)

    df_rawfile_input = df_rawfile_input.withColumn('last_sellg_txt',date_format(when(trim(col('last_sellg_txt')) == 'NOT APPLICABLE', '1991-04-15').otherwise(trim(col('last_sellg_txt'))), 'yyyy-MM-dd'))

    df_rawfile_input.createOrReplaceTempView("df_rawfile_input")

    # Remove the duplicates from the df_rawfile_input DataFrame
    df_final = spark.sql("""
    WITH row_deduplicated AS (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY pg_categ_txt, pg_super_categ_txt, upc_txt 
                ORDER BY last_sellg_txt DESC
            ) AS row_number_deduplicator_column
        FROM df_rawfile_input
    )
    SELECT *
    FROM row_deduplicated
    WHERE row_number_deduplicator_column = 1
    AND pg_categ_txt IS NOT NULL""")

    # Drop the unwanted columns
    df = df_final.drop("row_number_deduplicator_column")


    # Grouping the Product Data
    # Aggregate the Product Data and Grouping by pg_categ_txt, pg_super_categ_txt
    df_agg = df.groupBy("pg_categ_txt", "pg_super_categ_txt").agg(
        # expr(
        #     "CASE WHEN COUNT(DISTINCT base_desc_txt) = 1 THEN MIN(base_desc_txt) ELSE NULL END"
        # ).alias("base_desc_txt"),
        expr(
            "CASE WHEN COUNT(DISTINCT pg_brand_txt) = 1 THEN MIN(pg_brand_txt) ELSE NULL END"
        ).alias("pg_brand_txt"),
        # expr(
        #     "CASE WHEN COUNT(DISTINCT pg_gbu_txt) =1 THEN MIN( pg_gbu_txt) ELSE NULL END"
        # ).alias("pg_gbu_txt"),
        expr(
            "CASE WHEN COUNT(DISTINCT pg_conc_txt) =1 THEN MIN( pg_conc_txt) ELSE NULL END"
        ).alias("pg_conc_txt"),
        expr(
            "CASE WHEN COUNT(DISTINCT pg_grp_size_txt) =1 THEN MIN( pg_grp_size_txt) ELSE NULL END"
        ).alias("pg_grp_size_txt"),
        expr(
            "CASE WHEN COUNT(DISTINCT pg_mfgr_txt) =1 THEN MIN( pg_mfgr_txt) ELSE NULL END"
        ).alias("pg_mfgr_txt"),
        expr(
            "CASE WHEN COUNT(DISTINCT pg_mega_categ_txt) =1 THEN MIN( pg_mega_categ_txt) ELSE NULL END"
        ).alias("pg_mega_categ_txt"),
        expr(
            "CASE WHEN COUNT(DISTINCT pg_sectr_txt) =1 THEN MIN( pg_sectr_txt) ELSE NULL END"
        ).alias("pg_sectr_txt"),
        expr(
            "CASE WHEN COUNT(DISTINCT pg_seg_txt) =1 THEN MIN( pg_seg_txt) ELSE NULL END"
        ).alias("pg_seg_txt"),
        expr(
            "CASE WHEN COUNT(DISTINCT pg_sku_base_desc_txt) =1 THEN MIN( pg_sku_base_desc_txt) ELSE NULL END"
        ).alias("pg_sku_base_desc_txt"),
        # expr(
        #     "CASE WHEN COUNT(DISTINCT pg_sku_base_size_txt) =1 THEN MIN( pg_sku_base_size_txt) ELSE NULL END"
        # ).alias("pg_sku_base_size_txt"),
        expr(
            "CASE WHEN COUNT(DISTINCT pg_sku_num_txt) =1 THEN MIN( pg_sku_num_txt) ELSE NULL END"
        ).alias("pg_sku_num_txt"),
        expr(
            "CASE WHEN COUNT(DISTINCT pg_sub_brand_txt) =1 THEN MIN( pg_sub_brand_txt) ELSE NULL END"
        ).alias("pg_sub_brand_txt"),
        expr(
            "CASE WHEN COUNT(DISTINCT pg_sub_mfgr_txt) =1 THEN MIN( pg_sub_mfgr_txt) ELSE NULL END"
        ).alias("pg_sub_mfgr_txt")
    )

    # Complementing df with df_agg
    df = df.unionByName(df_agg,allowMissingColumns=True)


    # Standardize the product data and add the key columns RUN_ID and srce_sys_id.
    df.createOrReplaceTempView("df")
    df = spark.sql(f"""
    SELECT 
    ref_df.prod_skid,
    in_df.* 
    FROM (
    SELECT 
        CASE 
        WHEN upc_txt IS NOT NULL AND pg_super_categ_txt IS NOT NULL 
            THEN upc_txt || ';' || pg_categ_txt || ';' || pg_super_categ_txt
        WHEN upc_txt IS NOT NULL AND pg_super_categ_txt IS NULL 
            THEN upc_txt || ';' || pg_categ_txt || ';NOT APPLICABLE'
        WHEN upc_txt IS NULL AND pg_super_categ_txt IS NOT NULL 
            THEN pg_categ_txt || ';' || pg_super_categ_txt
        ELSE pg_categ_txt 
        END AS extrn_prod_id,
        :run_id AS run_id,
        0 AS cntrt_id,
        :srce_sys_id AS srce_sys_id,
        * 
    FROM df
    ) AS in_df
    LEFT OUTER JOIN (
    SELECT * 
    FROM {catalog_name}.internal_tp.tp_prod_sdim
    WHERE part_srce_sys_id = :srce_sys_id 
    ) AS ref_df
    ON in_df.extrn_prod_id == ref_df.extrn_prod_id  and in_df.srce_sys_id =ref_df.srce_sys_id
    where ref_df.extrn_prod_id IS NULL
    """, {"srce_sys_id" : srce_sys_id, "run_id" : run_id })
    return df

def t2_publish_product(df,catalog_name,schema_name, prod_dim, spark):
    df_connect_mm_prod_dim_vw = spark.sql(f"SELECT * FROM {catalog_name}.{schema_name}.{prod_dim} limit 0")
    df = df.unionByName(df_connect_mm_prod_dim_vw,allowMissingColumns=True)
    df.createOrReplaceTempView("df_mm_prod_sdim_promo_vw")

    merge_sql_sdim = f"""
    MERGE INTO {catalog_name}.{schema_name}.{prod_dim} tgt
    USING df_mm_prod_sdim_promo_vw src
    ON src.srce_sys_id = tgt.srce_sys_id AND src.prod_skid = tgt.prod_skid
    WHEN MATCHED THEN
    UPDATE SET *
    WHEN NOT MATCHED THEN
    INSERT *
    """
    spark.sql(merge_sql_sdim)

def release_semaphore(catalog_name,run_id,lock_path, spark ):
    spark.sql(f"DELETE FROM {catalog_name}.internal_tp.tp_run_lock_plc WHERE run_id = :run_id AND lock_path IN (':lock_path')", {"run_id" : run_id, "lock_path" : lock_path})


def read_from_postgres(object_name, spark, refDBjdbcURL, refDBname, refDBuser, refDBpwd ):

    """
    Reads data from a PostgreSQL table into a Spark DataFrame.
    
    Parameters:
    object_name (str): The name of the table to read from.
    
    Returns:
    DataFrame: A Spark DataFrame containing the data from the specified table.
    """
    df = spark.read.format("jdbc") \
        .option("driver", "org.postgresql.Driver") \
        .option("url", f"{refDBjdbcURL}/{refDBname}") \
        .option("dbtable", f'''{object_name}''') \
        .option("user", f"{refDBuser}") \
        .option("password", f"{refDBpwd}") \
        .option("ssl", True) \
        .option("sslmode", "require") \
        .option("sslfactory", "org.postgresql.ssl.NonValidatingFactory") \
        .load()
    return df

def read_query_from_postgres(query, spark, refDBjdbcURL, refDBname, refDBuser, refDBpwd):

    """
    Reads data from a PostgreSQL table using a query into a Spark DataFrame.
    
    Parameters:
    query (str): The SQL query to execute.
    
    Returns:
    DataFrame: A Spark DataFrame containing the data using the query.
    """
    df = spark.read.format("jdbc") \
        .option("driver", "org.postgresql.Driver") \
        .option("url", f"{refDBjdbcURL}/{refDBname}") \
        .option("query", query) \
        .option("user", f"{refDBuser}") \
        .option("password", f"{refDBpwd}") \
        .option("ssl", True) \
        .option("sslmode", "require") \
        .option("sslfactory", "org.postgresql.ssl.NonValidatingFactory") \
        .load()
    return df

def write_to_postgres(df, object_name, spark, refDBjdbcURL, refDBname, refDBuser, refDBpwd):

    """
    Writes data from a Spark DataFrame to a PostgreSQL table.
    
    Parameters:
    df (DataFrame): The Spark DataFrame to write.
    object_name (str): The name of the table to write to.
    """
    df.write.format("jdbc") \
        .option("url", f"{refDBjdbcURL}/{refDBname}") \
        .option("dbtable", f'''{object_name}''') \
        .option("user", f"{refDBuser}") \
        .option("password", f"{refDBpwd}") \
        .mode("append") \
        .save()


def update_to_postgres(query, spark, refDBjdbcURL, refDBname, refDBuser, refDBpwd):
    conn = psycopg2.connect(
        dbname=refDBname,
        user=refDBuser,
        password=refDBpwd,
        host="psql-pg-flex-tpconsole-dev-1.postgres.database.azure.com",
        sslmode='require'
    )
    cursor = conn.cursor()
    cursor.execute(query)
    conn.commit()
    cursor.close()
    conn.close()

def unzip(FILE_NAME,catalog_name, VOL_PATH):
    """
    Unzips a file specified by the FILE_NAME widget in the WORK directory of the Databricks catalog.
    The unzipped contents are extracted to a folder named after the file without the .zip extension.
    """
    # Import necessary libraries

    print(f'Started unzipping the file: {VOL_PATH}{FILE_NAME}')

    # Remove the .zip extension to create a folder name for extraction
    # filename_wo_extn = FILE_NAME.replace(".zip", "")

    filename_wo_extn = re.sub(r'\.zip$', '', FILE_NAME, flags=re.IGNORECASE)

    # Open the zip file and extract its contents to the target directory
    with zipfile.ZipFile(f'{VOL_PATH}{FILE_NAME}', 'r') as zip_ref:
        zip_ref.extractall(f'{VOL_PATH}{filename_wo_extn}')

    # Log the completion of the unzipping process
    print('Unzipping finished')

def add_secure_group_key(df, cntrt_id, postgres_schema, spark, refDBjdbcURL, refDBname, refDBuser, refDBpwd ):

    # Retrieve the schema name for the PostgreSQL database from key vault
    # postgres_schema = dbutils.secrets.get('tp_dpf2cdl', 'database-postgres-schema') # type: ignore

    # SQL query to fetch the secure_group_key for the given contract ID
    SGK_cntrt_id = f"""
    SELECT secure_group_key
    FROM {postgres_schema}.mm_cntrt_secur_grp_key_assoc_vw
    WHERE cntrt_id = '{cntrt_id}'
    """
    # Execute the query and collect the secure_group_key
    SGK_cntrt_id = read_query_from_postgres(SGK_cntrt_id, spark, refDBjdbcURL, refDBname, refDBuser, refDBpwd ).collect() # type: ignore

    SGK_default = f"""
        SELECT secure_group_key
        FROM {postgres_schema}.mm_secur_grp_key_lkp
        WHERE secur_grp_key_id IS NULL
        """
    SGK_default = read_query_from_postgres(SGK_default, spark, refDBjdbcURL, refDBname, refDBuser, refDBpwd ).collect() # type: ignore

    # Check if a secure_group_key was found for the given contract ID
    if SGK_cntrt_id and SGK_cntrt_id[0]['secure_group_key'] is not None:
        secure_group_key = SGK_cntrt_id[0]['secure_group_key']
    else:
        # If no secure_group_key found for cntrt_id, fetch the default secure_group_key from the mm_secur_grp_key_lkp table
        secure_group_key = SGK_default[0]['secure_group_key']

    return df.withColumn("secure_group_key", lit(secure_group_key).cast(LongType()))

################################################################################

def work_to_arch(RUN_ID,CNTRT_ID,FILE_NAME, dbutils):
    paths=dbutils.fs.ls('/mnt/tp-source-data/WORK') # type: ignore
    # print(paths)
    for fileName in paths:
        if (FILE_NAME==fileName.name): 
            print(fileName.name)
            dbutils.fs.mv('/mnt/tp-source-data/WORK/'+fileName.name,'/mnt/tp-source-data/ARCH/'+fileName.name) # type: ignore
            return True
    return False

###################################################################################
def load_file(file_type, RUN_ID, CNTRT_ID, STEP_FILE_PATTERN, vendor_pattern, notebook_name, delimiter, dbutils, postgres_schema, spark, refDBjdbcURL, refDBname, refDBuser, refDBpwd ):
    # Define paths for raw and light refined data
    RAW_PATH = "tp-source-data/WORK/"
    LIGHT_REFINED_PATH = 'tp-publish-data/'
    
    # Read column mappings from parquet file
    df_dpf_col_asign_vw = spark.read.parquet(f"/mnt/tp-source-data/temp/materialised/{RUN_ID}/column_mappings_df_dpf_col_asign_vw")
    
    # List files in the raw data path
    files = dbutils.fs.ls(f"/mnt/{RAW_PATH}")
    localpath = " "

    # Identify the local path of the ZIP file containing the RUN_ID
    for fi in files:
        filename = fi.name
        filepath = fi.path    
        endwithzip = filename.endswith('.zip')
        endwithgz = filename.endswith('.gz')
        endwithcsv = filename.endswith('.csv')
    
        if ((endwithzip) and (str(RUN_ID) in filename)):  # If input file is a ZIP file
            localpath = os.path.join(f"/dbfs/mnt/{RAW_PATH}", filename)

    # Prepare file name patterns
    s = STEP_FILE_PATTERN
    position = s.find('%')
    file_name_for_zip = s.replace('%', '*')
    file_name = s[0:position] + f'*{RUN_ID}*'

    # Read the external data file based on the file type and local path
    if ".zip" in localpath:
        df_extrn = spark.read.format('csv').option('header', True).option('delimiter', delimiter).load(f"/mnt/{RAW_PATH}/*{RUN_ID}*/{file_name_for_zip}")
    else:
        df_extrn = spark.read.format('csv').option('header', True).option('delimiter', delimiter).load(f'/mnt/{RAW_PATH}/{file_name}')

    if file_type == 'fact':
        if ".zip" in localpath:
            df_extrn_raw = spark.read.format('csv').option('header', True).option('delimiter', delimiter).load(f'/mnt/{RAW_PATH}/*{RUN_ID}*/{file_name_for_zip}*')
        else:
            df_extrn_raw = spark.read.format('csv').option('header', True).option('delimiter', delimiter).load(f'/mnt/{RAW_PATH}/{file_name}')

    # Strip whitespace from column names
    for i in df_extrn.columns:
        df_extrn = df_extrn.withColumnRenamed(i, i.strip())

    if file_type == 'fact':
        for i in df_extrn_raw.columns:
            df_extrn_raw = df_extrn_raw.withColumnRenamed(i, i.strip())
    if file_type != 'fact':
        df_extrn_raw = df_extrn
    
    # Define list of columns based on file type
    if file_type == 'prod':
        list_cols = ['PROD', 'product', 'prod']
    elif file_type == 'mkt':
        list_cols = ['MKT', 'market', 'mkt']
    elif file_type == 'fact':
        list_cols = ['FACT', 'fact']
    elif file_type == 'time':
        list_cols = ['TIME', 'time']
    else:
        list_cols = []

    # Filter column mappings based on contract ID and table type
    df_dpf_col_asign_vw = df_dpf_col_asign_vw.filter(col('cntrt_id') == CNTRT_ID).where(df_dpf_col_asign_vw['dmnsn_name'].rlike("|".join(["(" + column + ")" for column in list_cols])))
    df_dpf_col_asign_vw = df_dpf_col_asign_vw.select("file_col_name", "db_col_name").distinct()

    df_dpf_col_asign_vw1 = df_dpf_col_asign_vw

    # Create a temporary view for column assignments
    df_dpf_col_asign_vw.createOrReplaceTempView("col_asign")
    
    df_dpf_col_asign_vw = spark.sql('''
        SELECT t.file_col_name, array_join(collect_list(t.db_col_name), ',') AS db_col_name
        FROM (
            SELECT file_col_name, db_col_name
            FROM col_asign
            ORDER BY file_col_name, db_col_name
        ) t
        GROUP BY t.file_col_name
    ''')
    

    # Create a dictionary for column mappings
    dictionary = {row['file_col_name']: row['db_col_name'] for row in df_dpf_col_asign_vw.collect()}
    
    for key, value in dictionary.items():
        if "," in value:
            lstValues = value.split(",")
            for v in lstValues:
                df_extrn = df_extrn.withColumn(v, col(f'`{key}`'))
            df_extrn = df_extrn.drop(key)
        else:
            df_extrn = df_extrn.withColumnRenamed(key, value)
    # display(df_extrn)
    # Drop extra columns not in the user-mapped columns
    df_cols = df_extrn.columns
    column = []
    for item in dictionary:
        if "," in dictionary[item]:
            lstItems = dictionary[item].split(",")
            for v in lstItems:
                column.append(v)
        else:
            column.append(dictionary[item])
    column = [n for n in column if len(n) != 0]

    drop_cols = [col for col in df_cols if col not in column]
    df_extrn = df_extrn.drop(*drop_cols)
    lstDropRowCols = df_extrn.columns
    
    # Drop rows with all measure columns null for non-fact files
    if file_type != 'fact':
        #df_measr = spark.read.parquet(f"/mnt/{LIGHT_REFINED_PATH}MM_MEASR_LKP_VW/")
        df_measr=read_query_from_postgres(f"SELECT *  FROM {postgres_schema}.mm_measr_lkp", spark, refDBjdbcURL, refDBname, refDBuser, refDBpwd )
        #lstMeasrs = [row['measr_phys_name'] for row in df_measr.where('fact_type_code = "TP"').select('measr_phys_name').distinct().collect()]
        lstMeasrs = [row['measr_phys_name'] for row in df_measr.select('measr_phys_name').distinct().collect()]
        drop_null_measr_cols = [col for col in lstDropRowCols if col in lstMeasrs]
        df_extrn = df_extrn.dropna(thresh=len(drop_null_measr_cols), subset=(drop_null_measr_cols))

    # Handle columns with special characters
    df1 = df_dpf_col_asign_vw1.filter("db_col_name like '%#%'").select("db_col_name")
    df2 = df1.withColumn("db_col_name", F.regexp_replace(F.col("db_col_name"), "[0-9#\s]", "")).distinct()

    l1 = df1.select("db_col_name").collect()
    l1 = [row["db_col_name"] for row in l1]
    l2 = df2.select("db_col_name").collect()
    l2 = [row["db_col_name"] for row in l2]

    for z in l2:
        lst = []
        for x in l1:
            if x.startswith(z):
                lst.append(x)
        lst_cols1 = sorted(lst)
        if file_type == 'fact':
            lst_cols1 = ['`' + x + '`' for x in lst_cols1]

        for d in lst_cols1:
            #if file_type != 'fact':
            df_extrn = df_extrn.withColumn(d, when(col(d).isNull(), '').otherwise(col(d)))
            #elif file_type == 'fact':
                #df_extrn = df_extrn.withColumn(z, F.expr('+'.join(lst_cols1)))
        #if file_type != 'fact':
        df_extrn = df_extrn.withColumn(z, concat_ws(':', *lst_cols1))
        df_extrn = df_extrn.drop(*lst_cols1)
    df_extrn = df_extrn.drop(*l1)

    # Convert column names to lowercase
    for i in df_extrn.columns:
       df_extrn = df_extrn.withColumnRenamed(i, i.lower())

    # Remove duplicate rows for non-fact files
    if file_type != 'fact':
        df_extrn = df_extrn.distinct()

    # Save the processed DataFrame to parquet files
  
    # display(df_extrn_raw)
    df_extrn.write.mode("overwrite").format('parquet').save(f"/mnt/tp-source-data/temp/materialised/{RUN_ID}/{notebook_name}_df_{file_type}_extrn")
    df_extrn_raw.write.mode("overwrite").format('parquet').save(f"/mnt/tp-source-data/temp/materialised/{RUN_ID}/{notebook_name}_df_{file_type}_extrn_raw")
    if file_type == 'fact':
        df_extrn.write.mode("overwrite").format('parquet').save(f"/mnt/tp-source-data/temp/materialised/{RUN_ID}/{notebook_name}_df_{file_type}_extrn_dvm")
    
    return 'Success'

############################################################################

def semaphore_queue(RUN_ID,paths, catalog_name, spark):
  
    # Create a comma-separated string of paths
    check_path = ', '.join(f"'{i}'" for i in paths)
    # Create a DataFrame with the paths
    paths_df = spark.createDataFrame([Row(lock_path=path) for path in paths])
    
    # Add additional columns to the DataFrame
    paths_df = paths_df.withColumn("run_id", lit(RUN_ID).cast("bigint")) \
                       .withColumn("lock_sttus", lit(False)) \
                       .withColumn("creat_date", current_timestamp())
    paths_df.createOrReplaceTempView("paths_df")
    # Insert data into the tp_run_lock_plc table
    table_name = f"{catalog_name}.internal_tp.tp_run_lock_plc"
    paths_df.write.format("delta").mode("append").saveAsTable(table_name)
    print("Data inserted successfully into", table_name)
    return check_path

#############################################################################

def check_lock(RUN_ID, check_path, catalog_name, spark):
    # Check for existing locks in the tp_run_lock_plc table
    df = spark.sql(f"""
        SELECT * 
        FROM paths_df curr 
        JOIN (
            SELECT run_id, lock_path 
            FROM {catalog_name}.internal_tp.tp_run_lock_plc 
            QUALIFY ROW_NUMBER() OVER(PARTITION BY lock_path ORDER BY creat_date)=1
        ) tbl 
        ON curr.run_id != tbl.run_id AND tbl.lock_path = curr.lock_path
    """)

    # If no locks are found, update the lock status to true
    if df.count() == 0:
        spark.sql(f"""
            UPDATE {catalog_name}.internal_tp.tp_run_lock_plc 
            SET lock_sttus = true 
            WHERE run_id = {RUN_ID} AND lock_path IN ({check_path})
        """)
        print("Semaphore Acquired by the current process")
        return check_path
    else:
        # Wait for the lock to be released and retry
        print('Waiting for lock')
        time.sleep(30)
        return check_lock(RUN_ID, check_path,catalog_name, spark)
    
###############################################################################

def semaphore_acquisition(RUN_ID, PATHS, catalog_name, spark):
    # Acquire semaphore by queuing and checking locks
    check_path = semaphore_queue(RUN_ID, PATHS, catalog_name, spark)
    check_path = check_lock(RUN_ID, check_path, catalog_name, spark)
    return check_path

################################################################################

def assign_skid(df, run_id, type, catalog_name,spark):
    try:

        # Convert the type to lowercase for consistency
        type = type.lower()

        # Proceed only if the type is either 'prod' or 'mkt'
        if type in ('prod', 'mkt'):
            # Register the input DataFrame as a temporary SQL view
            df.createOrReplaceTempView('input_df')

            # Store original column names for later use
            cols = df.columns

            # Select run_id and external ID (based on type) as key
            query = f"""
                SELECT CAST(run_id AS BIGINT), extrn_{type}_id AS key 
                FROM input_df
            """

            df_sel = spark.sql(query)

            # Check if the run_id already exists in the target table
            query = f"""
                SELECT * FROM {catalog_name}.internal_tp.tp_{type}_skid_seq 
                WHERE run_id = :run_id_param
                LIMIT 1
            """
            if spark.sql(query,{"run_id_param": run_id }).count() == 0:
                # If not, append the selected data to the target table
                df_sel.write.mode('append').saveAsTable(f'{catalog_name}.internal_tp.tp_{type}_skid_seq')

            # Read the skid mapping for the given run_id
            read_query = f"""
                SELECT {type}_skid, run_id, key 
                FROM {catalog_name}.internal_tp.tp_{type}_skid_seq 
                WHERE run_id = :run_id_param
            """
            df_sel = spark.sql(read_query,{"run_id_param": run_id })

            # Register both input and skid DataFrames as temporary views
            df.createOrReplaceTempView('input_df')
            df_sel.createOrReplaceTempView('skid_df')

            # Join input data with skid mapping to assign skid values
            query = f"""
                SELECT a.* EXCEPT(a.{type}_skid), b.{type}_skid 
                FROM input_df a 
                JOIN skid_df b 
                ON a.extrn_{type}_id = b.key 
                AND a.run_id = b.run_id
            """
            df = spark.sql(query).select(*cols)

            return df

    except Exception as e:
        # Raise any exceptions encountered during processing
        raise e
    
#####################################################################

def cdl_publishing(logical_table_name,physical_table_name,unity_catalog_table_name,partition_definition_value, dbutils):

    config = Configuration.load_for_default_environment_notebook(dbutils=dbutils)
    meta_client = MetaPSClient.configure(config).get_client()
    p_file_type_code = "delta"
    # get the tables from the config and iterate over them calling the publish_table method
    tables = config["tables"]
    for t in tables:
        (
        meta_client
        .mode(publish_mode="update")
        .publish_table(
            logical_table_name=logical_table_name,     #TP_WK_FCT
            physical_table_name=physical_table_name,    #TP_WK_FCT
            unity_catalog_table_name=unity_catalog_table_name,   #TP_WK_FCT
            unity_catalog_schema="gold_tp", 
            data_signals=[6888],
            partition_definition_value=partition_definition_value,
            file_type_code=p_file_type_code,
            data_type_code="TP",
            data_provider_code="TP",
            secure_group_key=0,
            test_only=False
            )
        )
    # start the publishing process, which will trigger the publication of all tables defined above 
    meta_client.start_publishing()