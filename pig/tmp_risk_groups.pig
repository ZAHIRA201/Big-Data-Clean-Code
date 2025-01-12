REGISTER /usr/local/pig/scripts/udf/udf-1.0.jar;
REGISTER /usr/local/pig/scripts/udf/fuzzywuzzy-1.4.0.jar

tmp_risk_classified = LOAD 'hdfs:///pig/1/tmp_risk_classified'
    USING org.apache.pig.piggybank.storage.CSVExcelStorage(
        ',', -- DELIMITER
        'NO_MULTILINE', -- NO MULTILINE
        'NOCHANGE', -- MULTILINE SPEC SYSTEM DEFAULT
        'SKIP_INPUT_HEADER' -- FILE CONTAINS HEADER
    )
    AS (
        industry: chararray,
        injuries_per_employee: double,
        deaths_per_employee: double,
        risk_group: chararray,
        total_injuries: int
    );

tmp_grouped = group tmp_risk_classified BY (industry, risk_group);
out_risk_groups = FOREACH tmp_grouped GENERATE
    group.industry as industry,
    group.risk_group as risk_group,
    COUNT(tmp_risk_classified) as repetitions,
    AVG(tmp_risk_classified.injuries_per_employee) as avg_injuries_per_employee:double,
    AVG(tmp_risk_classified.deaths_per_employee) as avg_deaths_per_employee:double;

fs -mkdir -p /pig/1/OUT_RISK_GROUPS;
fs -rm -r /pig/1/OUT_RISK_GROUPS;
STORE out_risk_groups INTO '/pig/1/OUT_RISK_GROUPS' using
    org.apache.pig.piggybank.storage.CSVExcelStorage(';');