/*

The Python SP below takes three arguments: a table name (incl. schema), the name of the Variant/JSON col to be checked &
the number of docs/ rows to check (less rows for faster processing, but eventually less accurate results)

-> The provided col gets normalized (keys of all levels -> cols)
-> Count distinct values for each key
-> Infer dtypes for each key
-> Get some possible values for each key (max. 3, could be easily parameterized also the option to use whole table)

-> Results will be returned as Variant

TODO:
Fix problem if JSON contains keys where values are lists (maybe empty dicts or lists can also be a problem!)

*/

CREATE OR REPLACE PROCEDURE GET_INFO_ON_JSON_COL(TABLE_NAME STRING, JSON_COL STRING, NUM_ROWS INTEGER)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python', 'pandas', 'simplejson', 'numpy')
HANDLER = 'run'
EXECUTE AS CALLER
AS
$$

import pandas as pd
import simplejson as json
import numpy as np

def run(session, table_name, json_col, num_rows):

    res = {}
    
    try:
        tmp=session.sql(f"SELECT {json_col} FROM {table_name} LIMIT {num_rows};").collect()
        df=pd.DataFrame(tmp)
        df=pd.json_normalize(df[json_col].astype(str).map(json.loads)).infer_objects().replace('', np.nan)
        for col in df.columns:
            res[col] = {"CNT_DISTINCT":int(df[col].nunique()),
                        "PREDICTED DTYPE":str(df[col].dtype),
                        "EXAMPLES":df[df[col].notnull()][col].unique()[:3].tolist()}
                        
    except Exception as e:
        res["An error occurred"] = str(e)
    
    return res

$$;

CALL GET_INFO_ON_JSON_COL('CORE_DATA.GET_FEEDBACK_TBL', 'JSON_CONTENT', 100000);
SELECT DISTINCT OBJECT_KEYS($1) FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

CALL GET_INFO_ON_JSON_COL('RAW_DATA.RAW_ORDER_DATA_TBL', 'JSON_CONTENT', 1);