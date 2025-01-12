REGISTER /usr/local/pig/scripts/udf/udf-1.0.jar;
REGISTER /usr/local/pig/scripts/udf/fuzzywuzzy-1.4.0.jar

tmp_mix = LOAD 'hdfs:///pig/1/tmp_mix'
    USING org.apache.pig.piggybank.storage.CSVExcelStorage(
        ',', -- DELIMITER
        'NO_MULTILINE', -- NO MULTILINE
        'NOCHANGE', -- MULTILINE SPEC SYSTEM DEFAULT
        'SKIP_INPUT_HEADER' -- FILE CONTAINS HEADER
    )
    AS (
        tmp_companies::name: chararray,
        tmp_companies::industry: chararray,
        tmp_companies::size_range: chararray,
        tmp_companies::city: chararray,
        tmp_companies::total_employee_estimate: int,
        tmp_injuries::no_injuries_illnesses: int,
        tmp_injuries::company_name: chararray,
        tmp_injuries::establishment_name: chararray,
        tmp_injuries::city: chararray,
        tmp_injuries::total_deaths: int,
        tmp_injuries::total_injuries: int,
        match_ratio: int,
        match_weight: int,
        match_token: int
    );

tmp_risk_classified = FOREACH tmp_mix GENERATE
    tmp_companies::industry as industry,
    (double)tmp_injuries::total_injuries / (double)tmp_companies::total_employee_estimate as injuries_per_employee:double,
    (double)tmp_injuries::total_deaths / (double)tmp_companies::total_employee_estimate as deaths_per_employee:double,
    pdzd.miron.ClassifyRiskGroup(tmp_injuries::total_injuries) as risk_group:chararray,
    tmp_injuries::total_injuries as total_injuries:int;

fs -mkdir -p /pig/1/tmp_risk_classified;
fs -rm -r /pig/1/tmp_risk_classified;
STORE tmp_risk_classified INTO '/pig/1/tmp_risk_classified' using
    org.apache.pig.piggybank.storage.CSVExcelStorage(',');