#!/bin/bash


## To run this:
##      chmod a+x udf_example.sql
##      ./udf_example.sql

bq query \-\-nouse_legacy_sql \




#for testing purposes
#How many times has source (LRdryden001) occured in touchpoint_type(Phone)?*/
'SELECT COUNT(*) AS count_LRdryden001_email
FROM `abilitec-build-staging-us.abilitec_build_interns.scorecard_table_test`
WHERE touchpoint_type = 'PHONE' and LRdryden001 ='1'
LIMIT 10000
'




#counts all 1's in row
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





#created table that outputs sole contributor
'CREATE TEMP FUNCTION oneContributor(json_str STRING)
RETURNS STRING
LANGUAGE js AS """
    var row = JSON.parse(json_str);
    var sum_of_values = 0;
    var source = "";
    for (var p in row) {
        if( row.hasOwnProperty(p) ) {
            if (p!="touchpoint_value" && p!="touchpoint_type") {
                if (row[p]!="0") {
                    source = p;
                    sum_of_values = sum_of_values+1;
                    if ( sum_of_values>1 ) {
                      return "N";
                    }
                }
            }
        }
    }
    if (sum_of_values==1) {
      return source;
    } else {
      return "N";
    }
""";

CREATE TABLE `abilitec-build-staging-us.abilitec_build_interns`.tp_name_address_oneContribution AS
SELECT touchpoint_value, touchpoint_type, oneContributor(TO_JSON_STRING(t)) AS one_contribution
FROM `abilitec-build-staging-us.abilitec_build_interns.tp_name_address`  AS t'






#count_array udf
#ex: sourceB is the sole contributor for 1 record (1=1) and is mitigated by another source in another record (2=1)
#sourceC is mitigated by another source in another record (2=1)
'CREATE TEMP FUNCTION burstSource(json_str STRING)
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
   FROM `abilitec-build-staging-us.abilitec_build_interns.tp_name_address` AS t)
SELECT source, ARRAY_AGG(cnt ORDER BY sourceCount ASC) AS cnt_array
FROM (SELECT flattened_records.source, flattened_records.sourceCount, format("%d = %d", flattened_records.sourceCount,count(*)) as cnt
      FROM bursted_sources
      CROSS JOIN UNNEST(bursted_sources.bursted_source) AS flattened_records
      GROUP BY flattened_records.source, flattened_records.sourceCount)
GROUP BY source;'





#seperates array_column to two columns (contribution, count)
'CREATE TEMP FUNCTION burstSource(json_str STRING)
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
CREATE TABLE `abilitec-build-staging-us.abilitec_build_interns`.all_name_address_contributions AS
WITH bursted_sources AS
  (SELECT touchpoint_value, burstSource(TO_JSON_STRING(t)) AS bursted_source
   FROM `abilitec-build-staging-us.abilitec_build_interns.tp_name_address` AS t)
SELECT source, contribution, ARRAY_AGG(cnt_per_contribution ORDER BY sourceCount ASC) AS cnt_per_contribution
FROM (SELECT flattened_records.source, flattened_records.sourceCount, flattened_records.sourceCount as contribution, count(*) as cnt_per_contribution
      FROM bursted_sources
      CROSS JOIN UNNEST(bursted_sources.bursted_source) AS flattened_records
      GROUP BY flattened_records.source, flattened_records.sourceCount)
GROUP BY source, contribution
ORDER BY contribution
'





#Temporal contribution 
'
SELECT nameAddress, financialIndustryCount, healthcareIndustryCount, insuranceIndustryCount, mediaIndustryCount, retailIndustryCount , B.one_contribution, (CAST(financialIndustryCount AS INT64)+CAST(healthcareIndustryCount AS INT64)+CAST(insuranceIndustryCount AS INT64)+CAST(mediaIndustryCount AS INT64)) as total
FROM `abilitec-build-staging-us.abilitec_build_interns.temporalSignal_nameAddr` A 
INNER JOIN `abilitec-build-staging-us.abilitec_build_interns.contribution_nameAddress` B ON A.nameAddress = B.touchpoint_value
ORDER BY total desc'





#Temporal contribution counter
'
SELECT one_contribution, count(*) as counter
FROM `abilitec-build-staging-us.abilitec_build_interns.temporalSignal_singleSourced_nameAddress`
GROUP BY one_contribution
ORDER BY counter DESC
'





#sources counter of 1 contributor
'
SELECT one_contribution, count(*) as counter
FROM `abilitec-build-staging-us.abilitec_build_interns.contribution_nameAddress`
WHERE one_contribution != 'N'
GROUP BY one_contribution
ORDER BY counter DESC'



#sources that only contribute to 1 value in edge contribution (name_address)
'CREATE TABLE `abilitec-build-staging-us.abilitec_build_interns`.contribution_nameAddress AS
SELECT touchpoint_value, one_contribution
FROM `abilitec-build-staging-us.abilitec_build_interns.tp_name_address_oneContribution`
WHERE   one_contribution != 'N'
'




#join contrinution with temporal signals - one source AND which had some temporal signal > 0*/
'SELECT nameAddress, financialIndustryCount, healthcareIndustryCount, insuranceIndustryCount, mediaIndustryCount, retailIndustryCount , B.one_contribution, (CAST(financialIndustryCount AS INT64)+CAST(healthcareIndustryCount AS INT64)+CAST(insuranceIndustryCount AS INT64)+CAST(mediaIndustryCount AS INT64)+CAST(retailIndustryCount AS INT64)) as total
FROM `abilitec-build-staging-us.abilitec_build_interns.temporalSignal_nameAddr` A 
INNER JOIN `abilitec-build-staging-us.abilitec_build_interns.contribution_nameAddress` B ON A.nameAddress = B.touchpoint_value
ORDER BY total desc
'





#one_contribution with counter percentage
'SELECT one_contribution, counter,(counter * 100) / (SELECT SUM(counter) FROM `abilitec-build-staging-us.abilitec_build_interns.oneContribution_nameAddress_counter`) AS counter_Percentage
FROM `abilitec-build-staging-us.abilitec_build_interns.oneContribution_nameAddress_counter`
GROUP BY one_contribution, counter
ORDER BY counter_Percentage DESC'

#same as above (counter percentage) but more effective with over()
'SELECT one_contribution, counter, (counter * 100) / sum(counter) over() as contribution_percentage
FROM `abilitec-build-staging-us.abilitec_build_interns.oneContribution_nameAddress_counter`
GROUP BY one_contribution, counter
ORDER BY contribution_percentage DESC
'

