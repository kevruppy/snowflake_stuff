from snowflake.snowpark import Session

def compare_objects(obj1:str, obj2:str) -> str:
    
    """
    This functions compares two objects (tables).
    Basically it is a wrapper for the following type of SQL stmt (with additional checks):
    Given are two tables TBL1 & TBL2

    (SELECT * FROM TBL1 MINUS SELECT * FROM TBL2)
    UNION ALL
    (SELECT * FROM TBL2 MINUS SELECT * FROM TBL_1)
    
    """
    
    session = Session.builder.configs(connection_parameters).create()
    
    obj1 = obj1.upper()
    obj2 = obj2.upper()
    
    stmt = f"""SELECT ARRAY_AGG(DATA_TYPE) WITHIN GROUP (ORDER BY ORDINAL_POSITION)
               =
               LEAD(ARRAY_AGG(DATA_TYPE) WITHIN GROUP (ORDER BY ORDINAL_POSITION)) OVER(ORDER BY TABLE_NAME) AS RESULT
               FROM INFORMATION_SCHEMA.COLUMNS
               WHERE TABLE_NAME IN ('{obj1}', '{obj2}')
               GROUP BY TABLE_NAME
               LIMIT 1"""
    
    res = session.sql(stmt).collect()
    
    if len(res) == 0:
        result = f"THERE IS NEITHER AN OBJECT CALLED {obj1} NOR AN OBJECT CALLED {obj2}"
    elif res[0].asDict()["RESULT"] is None:
        stmt = session.sql(f"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME IN ('{obj1}', '{obj2}')").collect()
        if stmt[0].asDict()["TABLE_NAME"] == obj1:
            result = f"{obj2} does not exist"
        else:
            result = f"{obj1} does not exist"
    elif res[0].asDict()["RESULT"] == False:
        stmt = session.sql(f"SELECT DISTINCT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME IN ('{obj1}', '{obj2}') GROUP BY TABLE_NAME").collect()
        if len(stmt) > 1:
            result = "AMOUNT OF COLUMNS DIFFERS"
        else:
            stmt = f"""WITH TMP AS (
                       SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, ORDINAL_POSITION
                       FROM INFORMATION_SCHEMA.COLUMNS
                       WHERE TABLE_NAME IN ('{obj1}', '{obj2}')
                       ORDER BY TABLE_NAME, ORDINAL_POSITION)
                       SELECT OBJECT_CONSTRUCT('{obj1}', OBJECT_AGG(COLUMN_NAME, DATA_TYPE::VARIANT),
                                               '{obj2}', LEAD(OBJECT_AGG(COLUMN_NAME, DATA_TYPE::VARIANT)) OVER(ORDER BY TABLE_NAME))::STRING AS RESULT
                       FROM TMP
                       GROUP BY TABLE_NAME
                       LIMIT 1"""
            
            res = session.sql(stmt).collect()[0].asDict()["RESULT"]
            
            result = f"OBJECTS DIFFER IN TERMS OF DTYPES: {res}"

    else:
        stmt = f"SELECT COUNT(*) = (SELECT COUNT(*) FROM {obj1}) AS RESULT FROM {obj2}"
        res = session.sql(stmt).collect()[0].asDict()["RESULT"]
        
        if res == False:
            stmt = f"""SELECT CONCAT('OBJECTS DIFFER IN TERMS OF ROWS, SEE ROW COUNTS: ', LISTAGG(CONCAT_WS(': ', OBJ_NAME, CNT), ' | ')) AS RESULT
                       FROM (
                       SELECT '{obj1}' AS OBJ_NAME, COUNT(*) AS CNT FROM {obj1}
                       UNION ALL
                       SELECT '{obj2}' AS OBJ_NAME, COUNT(*) AS CNT FROM {obj2})"""
            
            result = session.sql(stmt).collect()[0].asDict()["RESULT"]
        
        else:
            stmt = f"""SELECT CASE WHEN COUNT(*) = 0 THEN 'OBJECTS ARE EQUAL.' ELSE 'OBJECTS DIFFER. ' || COUNT(*) || ' DIFFS DETECTED.' END AS RESULT
                       FROM ((SELECT * FROM {obj1} MINUS SELECT * FROM {obj2}) UNION ALL (SELECT * FROM {obj2} MINUS SELECT * FROM {obj1}))"""
            result = session.sql(stmt).collect()[0].asDict()["RESULT"]

    return result