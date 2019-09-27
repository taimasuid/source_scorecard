
#!/bin/bash


## To run this:
##      chmod a+x udf_burst.sql
##      ./udf_burst.sql

bq query \-\-nouse_legacy_sql \

'
CREATE TEMP FUNCTION burstSource(json_str STRING)
RETURNS ARRAY<STRUCT<source STRING, sourceCount INT64>>
LANGUAGE js AS """
    var row = JSON.parse(json_str);
    var n_sources = 0;
    var source_list = [];
    for (var p in row) {
        if( row.hasOwnProperty(p) ) {
            if (p != "touchpoint_value" && p != "touchpoint_type") {
                if (row[p] != "0") {
                    n_sources = n_sources+1;
                    source_list.push({"source": p,"sourceCount": 0})
                }
            }
        }
    }
    for (var i = 0; i < source_list.length; i++) { 
      source_list[i]["sourceCount"] = n_sources;
    }
    return source_list;
""";

WITH bursted_sources AS
  (SELECT touchpoint_value, burstSource(TO_JSON_STRING(t)) AS bursted_source
   FROM `abilitec-build-staging-us.abilitec_build_interns.udf_test` AS t)
SELECT source, contribution, ARRAY_AGG(cnt_per_contribution ORDER BY sourceCount ASC) AS cnt_per_contribution, contribution >=2 as more_than_five
FROM (SELECT flattened_records.source, flattened_records.sourceCount, flattened_records.sourceCount as contribution, count(*) as cnt_per_contribution
      FROM bursted_sources
      CROSS JOIN UNNEST(bursted_sources.bursted_source) AS flattened_records
      GROUP BY flattened_records.source, flattened_records.sourceCount)

GROUP BY source, contribution
ORDER BY contribution
'


