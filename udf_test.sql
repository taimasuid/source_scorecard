#!/bin/bash


## To run this:
##      chmod a+x udf_example.sql
##      ./udf_example.sql

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
FROM `abilitec-build-staging-us.abilitec_build_interns.udf_test`  AS t'
