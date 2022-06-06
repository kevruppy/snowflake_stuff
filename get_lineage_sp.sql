CREATE OR REPLACE PROCEDURE GET_LINEAGE(OBJ_SCHEMA VARCHAR, OBJ_NAME VARCHAR, OBJ_TYPE VARCHAR, DIRECTION VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    // capitalize arguments
    
    var obj_schema  = OBJ_SCHEMA.toUpperCase();
    var obj_name    = OBJ_NAME.toUpperCase();
    var obj_type    = OBJ_TYPE.toUpperCase();
    var direction   = DIRECTION.toUpperCase();
    
    // prepare variable to check if direction arg provided is valid
    
    switch(direction){
		case "BACKWARD":
			arg_check = "PASSED";
			break;
		case "FORWARD":
			arg_check = "PASSED";
			break;
		default:
			arg_check = "FAILED";
	};
    
    // backward_stmt
    
    var backward_lineage_tmp_tbl_stmt = "CREATE OR REPLACE TEMPORARY TABLE backward_lineage_tmp_tbl AS ("
                                      + "WITH RECURSIVE referenced_cte AS ( "
                                      + "SELECT CONCAT_WS(' < ', referenced_schema || '.' || referenced_object_name, referencing_schema || '.' || referencing_object_name) AS object_name_path "
                                      + ",referenced_schema, referenced_object_name, referenced_object_domain, referencing_schema, "
                                      + "referencing_object_name, referencing_object_domain ,referenced_object_id    ,referencing_object_id "
                                      + "FROM snowflake.account_usage.object_dependencies AS referencing "
                                      + "WHERE  referencing_schema          = '" + obj_schema   + "' "
                                      + "AND    referencing_object_name     = '" + obj_name     + "' "
                                      + "AND    referencing_object_domain   = '" + obj_type     + "' "
                                      + "UNION ALL "
                                      + "SELECT CONCAT_WS(' < ', referencing.referenced_schema || '.' ||referencing.referenced_object_name, referenced_cte.object_name_path) "
                                      + ",referencing.referenced_schema, referencing.referenced_object_name, referencing.referenced_object_domain, referencing.referencing_schema "
                                      + ",referencing.referencing_object_name, referencing.referencing_object_domain, referencing.referenced_object_id, referencing.referencing_object_id "
                                      + "FROM snowflake.account_usage.object_dependencies AS referencing "
                                      + "INNER JOIN referenced_cte "
                                      + "ON referencing.referencing_object_id = referenced_cte.referenced_object_id "
                                      + "AND referencing.referencing_object_domain = referenced_cte.referenced_object_domain) "
                                      + "SELECT DISTINCT object_name_path, referencing_schema, referencing_object_name, referencing_object_domain, "
                                      + "referenced_schema, referenced_object_name, referenced_object_domain "
                                      + "FROM referenced_cte);";

    // forward_stmt
    
    var forward_lineage_tmp_tbl_stmt = "CREATE OR REPLACE TEMPORARY TABLE forward_lineage_tmp_tbl AS ( "
                                     + "WITH RECURSIVE referenced_cte AS ( "
                                     + "SELECT CONCAT_WS('> ', referenced_schema || '.' || referenced_object_name, referencing_schema || '.' || referencing_object_name) AS object_name_path "
                                     + ",referenced_schema, referenced_object_name, referenced_object_domain, referencing_schema, referencing_object_name "
                                     + ",referencing_object_domain, referenced_object_id, referencing_object_id "
                                     + "FROM snowflake.account_usage.object_dependencies AS referencing "
                                     + "WHERE   referenced_schema           = '" + obj_schema    + "' "
                                     + "AND     referenced_object_name      = '" + obj_name      + "' "
                                     + "AND     referenced_object_domain    = '" + obj_type      + "' "
                                     + "UNION ALL "
                                     + "SELECT CONCAT_WS(' > ', referenced_cte.object_name_path, referencing.referencing_schema || '.' || referencing.referencing_object_name) "
                                     + ",referencing.referenced_schema, referencing.referenced_object_name, referencing.referenced_object_domain, referencing.referencing_schema "
                                     + ",referencing.referencing_object_name, referencing.referencing_object_domain, referencing.referenced_object_id, referencing.referencing_object_id "
                                     + "FROM snowflake.account_usage.object_dependencies AS referencing "
                                     + "INNER JOIN referenced_cte "
                                     + "ON referencing.referenced_object_id = referenced_cte.referencing_object_id "
                                     + "AND referencing.referenced_object_domain = referenced_cte.referencing_object_domain) "
                                     + "SELECT DISTINCT object_name_path, referenced_schema, referenced_object_name, referenced_object_domain, referencing_schema, referencing_object_name, referencing_object_domain "
                                     + "FROM referenced_cte);";
    
    // check existence of table or view
    
    var check_obj_existence = "WITH objects AS( "
                            + "SELECT table_type, CONCAT(table_schema, '.', table_name) AS full_obj_name "
                            + "FROM verivox_dwh.information_schema.tables) "
                            + "SELECT CASE WHEN COUNT(*) > 0 THEN 'PASSED' ELSE 'FAILED' END AS check_result "
                            + "FROM objects "
                            + "WHERE table_type LIKE '%" + obj_type + "%'"
                            + "AND full_obj_name = '" + obj_schema + "." + obj_name + "';";

    var check_obj_existence_exec = snowflake.createStatement({sqlText:check_obj_existence}).execute();
    check_obj_existence_exec.next();
    
    if(check_obj_existence_exec.getColumnValue(1) === "PASSED")
    {
        if(arg_check === "PASSED")
        {
        
            if(direction === "BACKWARD")
            {
    
            var backward_lineage_tmp_tbl_stmt_exec = snowflake.createStatement({sqlText:backward_lineage_tmp_tbl_stmt}).execute();
            return "GET RESULTS BY EXECUTING THIS QUERY: SELECT * FROM BACKWARD_LINEAGE_TMP_TBL;"
    
            } else {
    
            var forward_lineage_tmp_tbl_stmt_exec = snowflake.createStatement({sqlText:forward_lineage_tmp_tbl_stmt}).execute();
            return "GET RESULTS BY EXECUTING THIS QUERY: SELECT * FROM FORWARD_LINEAGE_TMP_TBL;"
    
            }
            
        } else {
        
        return "INVALID ARGUMENT: DIRECTION MUST BE EITHER BACKWARD OR FORWARD"
        
        }
    
    } else {
    
    return "THERE IS NO " + obj_type + " CALLED " +  obj_schema + "." + obj_name
    
    }   
$$

/*
### EXAMPLES ###

-- EXAMPLE 1: FORWARD SEARCH

CALL GET_LINEAGE('PUBLIC', 'ORDER_TBL', 'TABLE', 'FORWARD');
SELECT * FROM FORWARD_LINEAGE_TMP_TBL;

-- EXAMPLE 2: BACKWARD SEARCH
-- BTW: THE ARGUMENTS OF THE SP ARE NOT CASE SENSITIVE

CALL GET_LINEAGE('PUBLIC', 'SEA_PERFORMANCE', 'View', 'backward');
SELECT * FROM BACKWARD_LINEAGE_TMP_TBL;

-- EXAMPLE 3: OBJECT DOES NOT EXIST
-- NO ERROR IS BEING THROWN INSTEAD A MESSAGE IS PROVIDED

CALL GET_LINEAGE('PUBLIC', 'ORDER_TBL', 'VIEW', 'FORWARD');

-- EXAMPLE 4: OBJ_TYPE ARGUMENT IS NOT VALID
-- NO ERROR IS BEING THROWN INSTEAD A MESSAGE IS PROVIDED

CALL GET_LINEAGE('PUBLIC', 'ORDER_TBL', 'XXX', 'FORWARD');

-- EXAMPLE 5: DIRECTION ARGUMENT IS NEITHER 'FORWARD' NOR 'BACKWARD'
-- NO ERROR IS BEING THROWN INSTEAD A MESSAGE IS PROVIDED

CALL GET_LINEAGE('PUBLIC', 'ORDER_TBL', 'TABLE', '???');

*/
;