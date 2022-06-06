CREATE OR REPLACE PROCEDURE CHECK_FOR_LONG_RUNNING_QUERIES_SP(END_TIME_RANGE_START_NUMBER VARCHAR, THRESHOLD VARCHAR, SNS_ARGS VARCHAR)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
/*
CALL CHECK_FOR_LONG_RUNNING_QUERIES(
'48',
'1000',
'{"sns_topic": "arn:aws:sns:eu-central-1:725608425951:SnowflakeMail", "mail_subject":"AT LEAST ONE LONG RUNNING QUERY DETECTED"}',
);
*/
$$

// initialize result to be returned

var result  = {};

// parse variables

var end_time_range_start_number = String(END_TIME_RANGE_START_NUMBER);
var threshold                   = String(THRESHOLD);
var sns_args                    = JSON.parse(SNS_ARGS);


// stmt to be executed

var stmt    = "SELECT ARRAY_AGG(OBJECT_CONSTRUCT('-> QUERY_ID', QUERY_ID,"
            + "'-> ROLE_NAME', ROLE_NAME,"
            + "'-> WAREHOUSE_NAME', WAREHOUSE_NAME,"
            + "'-> EXECUTION_STATUS', EXECUTION_STATUS,"
            + "'-> START_TIME', START_TIME::STRING,"
            + "'-> CURRENT_EXECUTION_TIME_IN_SEC', DATEDIFF(SECOND, START_TIME, CURRENT_TIMESTAMP()),"
            + "'-> USER_NAME', USER_NAME,"
            + "'QUERY_TEXT', CASE WHEN LEN(QUERY_TEXT) > 500 THEN CONCAT(LEFT(QUERY_TEXT, 500), '...') ELSE LEFT(QUERY_TEXT, 500) END)) AS OBJ"
            + ",CASE WHEN ARRAY_SIZE(OBJ) > 0 THEN 'YES' ELSE 'NO' END AS SEND_MAIL"
            + " FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(END_TIME_RANGE_START => DATEADD(HOUR, " + end_time_range_start_number + ", CURRENT_TIMESTAMP())))"
            + " WHERE EXECUTION_STATUS IN ('RUNNING', 'BLOCKED')"
            + " AND DATEDIFF(SECOND, START_TIME, CURRENT_TIMESTAMP()) >= " + threshold + ";";

// execute stmt & send mail if result set is not empty

try {
    var stmt_exec   = snowflake.createStatement({sqlText:stmt});
    var stmt_result = stmt_exec.execute();
    stmt_result.next();
    
    if (stmt_result.getColumnValue("SEND_MAIL") === "YES") {
        
        // get result
        
        result["RESULT"] = stmt_result.getColumnValue("OBJ");
        
        // sns
        
        sns_topic       = sns_args["sns_topic"];
        mail_subject    = sns_args["mail_subject"];
        custom_message  = "At least one long running query deteced: "
        message         = JSON.stringify(result, null, 2);
		
		// this is using an external udf!
		
        stmt_send_sns = "SELECT SEND_LOG_SNS("
					  + "'" + sns_topic       + "',"
					  + "'" + mail_subject    + "',"
					  + "'" + custom_message  +"',"
					  + "\$\$" + message + "\$\$"
				      + ");";
        
        stmt_send_sns_exec = snowflake.createStatement({sqlText: stmt_send_sns});
        stmt_send_sns_exec.execute();
        
        //
        
        return result
   
    } else {
    
        result["args"] = {"end_time_range_start_number":end_time_range_start_number, "threshold":threshold};
        result["msg"] = "No long running queries found. Check used arguements and eventually try again.";
        result["stmt"] = stmt;

        return result;
        
    };
    
   } catch (err) {
    
     result["args"] = {"end_time_range_start_number":end_time_range_start_number, "threshold":threshold};
     result["msg"] = "Something went wrong! Please check with which arguments the procedure was called.";
     result["stmt"] = stmt;
     
     return result;
    
    };
$$;