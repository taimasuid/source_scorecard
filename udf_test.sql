#!/bin/bash


## To run this:
##      chmod a+x udf_test.sql
##      ./udf_test.sql

bq query \-\-nouse_legacy_sql \


#count all 1's in row

'CREATE TEMP FUNCTION counter(json_str STRING)
RETURNS STRING
LANGUAGE js AS """
    var row = JSON.parse(json_str);
    var count = 0;
    for (var p in row) {
        if( row.hasOwnProperty(p) ) {
            if (p!="touchpoint_value" && p!="touchpoint_type") {
                if (row[p]!="1") {
                  count = 0;
                } else {
                  count = count+1;
                  return count;
                }
            }
        }
    }
""";


SELECT touchpoint_value, touchpoint_type, counter(TO_JSON_STRING(t)) AS counter
FROM `[LOCATION].[TABLE NAME]`  AS t'
