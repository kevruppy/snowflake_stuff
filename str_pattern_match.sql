/*
* Returns true if a string matches to a list of reg exp patterns
* 
* Example:
*
* select
*  str_pattern_match(ARRAY_CONSTRUCT('^c:brand$'), 'c:brand', 'i') as MATCH_TRUE,
*  str_pattern_match(ARRAY_CONSTRUCT('^c:brand$'), 'c:brands', 'i') as MATCH_FALSE
* 
*/


CREATE OR REPLACE FUNCTION STR_PATTERN_MATCH(PATTERN_ARR ARRAY, STR STRING, REGEXP_PARAM STRING)
RETURNS BOOLEAN
LANGUAGE JAVASCRIPT
AS
$$
              var result;
              for (var i = 0; i < PATTERN_ARR.length; i++){
                            var pattern = new RegExp(PATTERN_ARR[i], REGEXP_PARAM);
                            var result = pattern.test(STR);
                            if (result === true) return result;
              }
              return result;
$$;
