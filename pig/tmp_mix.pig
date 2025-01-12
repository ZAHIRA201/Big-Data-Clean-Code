REGISTER /usr/local/pig/scripts/udf/udf-1.0.jar;
REGISTER /usr/local/pig/scripts/udf/fuzzywuzzy-1.4.0.jar

tmp_companies = LOAD 'hdfs:///pig/1/tmp_companies_extracted'
USING org.apache.pig.piggybank.storage.CSVExcelStorage(
    ',', --DELIMITER
    'NO_MULTILINE', --NO MULTILINE 
    'NOCHANGE', --MULTILINE SPEC SYSTEM DEFAULT
    'SKIP_INPUT_HEADER'--FILE CONTAINS HEADER
)
AS(
    name: chararray,
    industry: chararray,
    size_range: chararray,
    city: chararray,
    total_employee_estimate: int
);
tmp_injuries = LOAD 'hdfs:///pig/1/tmp_injuries_filtered'
USING org.apache.pig.piggybank.storage.CSVExcelStorage(
    ',', --DELIMITER
    'NO_MULTILINE', --NO MULTILINE 
    'NOCHANGE', --MULTILINE SPEC SYSTEM DEFAULT
    'SKIP_INPUT_HEADER'--FILE CONTAINS HEADER
)
AS(
    no_injuries_illnesses: int,
    company_name: chararray,
    establishment_name: chararray,
    city: chararray,
    total_deaths: int,
    total_injuries: int
);
-- This merger generates a lot of data and severly impacts stage's processing time.
tmp_join = JOIN tmp_companies BY city, tmp_injuries BY city;
tmp_fuzzed = FOREACH tmp_join
    GENERATE
    *,
    pdzd.miron.FuzzyMatchingRatio(tmp_companies::name,tmp_injuries::company_name) AS match_ratio: int,
    pdzd.miron.FuzzyMatchingWeight(tmp_companies::name,tmp_injuries::company_name) AS match_weight: int,
    pdzd.miron.FuzzyMatchingToken(tmp_companies::name,tmp_injuries::company_name) AS match_token: int;
tmp_mix = FILTER tmp_fuzzed
    BY((match_ratio > 83 OR match_weight > 86 OR match_token > 88) AND NOT(match_ratio < 65 OR match_token < 55));
fs -mkdir -p /pig/1/tmp_mix;
fs -rm -r /pig/1/tmp_mix;
STORE tmp_mix INTO '/pig/1/tmp_mix' using
    org.apache.pig.piggybank.storage.CSVExcelStorage(',');