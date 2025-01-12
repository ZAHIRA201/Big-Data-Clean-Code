REGISTER /usr/local/pig/scripts/udf/udf-1.0.jar;

companies = LOAD 'hdfs:///user/root/companies/COMPANIES.csv'
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
        locality: chararray,
        total_employee_estimate: int
    );
tmp_companies_extracted = FOREACH companies GENERATE
    name,
    (industry == '' ? 'Empty' : industry),
    size_range,
    pdzd.miron.CompaniesExtractLocation($3),
    total_employee_estimate;
tmp_companies_extracted = SAMPLE tmp_companies_extracted 0.1;

fs -mkdir -p /pig/1/tmp_companies_extracted;
fs -rm -r /pig/1/tmp_companies_extracted;
STORE tmp_companies_extracted INTO '/pig/1/tmp_companies_extracted' using
    org.apache.pig.piggybank.storage.CSVExcelStorage(',');