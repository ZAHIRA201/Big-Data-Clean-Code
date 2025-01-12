injuries = LOAD 'hdfs:///user/root/companies/INJURIES.csv'
    USING org.apache.pig.piggybank.storage.CSVExcelStorage(
        ',', --DELIMITER
        'NO_MULTILINE', --NO MULTILINE 
        'NOCHANGE', --MULTILINE SPEC SYSTEM DEFAULT
        'SKIP_INPUT_HEADER'--FILE CONTAINS HEADER
    )
    AS(
        company_name: chararray,
        establishment_name: chararray,
        city: chararray,
        state: chararray,
        no_injuries_illnesses: int,
        total_deaths: int,
        total_dafw_cases: int,
        total_djtr_cases: int,
        total_other_cases: int,
        total_injuries: int,
        total_poisonings: int,
        total_respiratory_conditions: int,
        total_skin_disorders: int,
        total_hearing_loss: int
    );

tmp_injuries_filtered = FILTER injuries BY no_injuries_illnesses == 1;
tmp_injuries_filtered = FOREACH tmp_injuries_filtered GENERATE
    no_injuries_illnesses,
    company_name,
    establishment_name,
    city,
    total_deaths,
    total_injuries;
tmp_injuries_filtered = SAMPLE tmp_injuries_filtered 0.1;

fs -mkdir -p /pig/1/tmp_injuries_filtered;
fs -rm -r /pig/1/tmp_injuries_filtered;
STORE tmp_injuries_filtered INTO '/pig/1/tmp_injuries_filtered' using
    org.apache.pig.piggybank.storage.CSVExcelStorage(',');