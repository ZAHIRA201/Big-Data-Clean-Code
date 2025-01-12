REGISTER /usr/local/pig/scripts/udf/udf-1.0.jar;
REGISTER /usr/local/pig/scripts/udf/fuzzywuzzy-1.4.0.jar

tmp_mix = LOAD 'hdfs:///pig/1/tmp_mix'
    USING org.apache.pig.piggybank.storage.CSVExcelStorage(
        ',', --DELIMITER
        'NO_MULTILINE', --NO MULTILINE 
        'NOCHANGE', --MULTILINE SPEC SYSTEM DEFAULT
        'SKIP_INPUT_HEADER'--FILE CONTAINS HEADER
    )
    AS(
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

tmp_grouped = group tmp_mix BY tmp_companies::size_range;
out_size_injuries = FOREACH tmp_grouped GENERATE group as size,
    AVG(tmp_mix.tmp_injuries::total_injuries) as avg_total_injuries,
    AVG(tmp_mix.tmp_injuries::total_deaths) as avg_total_deaths,
    AVG(tmp_mix.tmp_companies::total_employee_estimate) as avg_total_employee_estimate;

fs -mkdir -p /pig/1/OUT_SIZE_INJURIES;
fs -rm -r /pig/1/OUT_SIZE_INJURIES;
STORE out_size_injuries INTO '/pig/1/OUT_SIZE_INJURIES' using
    org.apache.pig.piggybank.storage.CSVExcelStorage(';');