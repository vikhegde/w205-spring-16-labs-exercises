set hive.cli.print.header=true;
DROP TABLE IF EXISTS raw_hospitals;
CREATE EXTERNAL TABLE raw_hospitals(provider_id VARCHAR(8),
    hospital_name VARCHAR(52),
    address VARCHAR(52),
    city VARCHAR(22),
    state VARCHAR(4),
    zip VARCHAR(7),
    county VARCHAR(22),
    phone VARCHAR(12),
    type VARCHAR(38),
    ownership VARCHAR(45),
    emergency VARCHAR(5))
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES(
 "separatorChar"=",",
 "quoteChar" = '"',
 "escapeChar" = '\\'
)
STORED AS TEXTFILE
LOCATION '/user/w205/hospital_compare/raw_hospitals_dir';

DROP TABLE IF EXISTS raw_effective_care;
CREATE EXTERNAL TABLE raw_effective_care(provider_id VARCHAR(8),
    hospital_name VARCHAR(52),
    address VARCHAR(46),
    city VARCHAR(22),
    state VARCHAR(4),
    zip VARCHAR(7),
    county VARCHAR(22),
    phone VARCHAR(12),
    condition VARCHAR(37),
    measure_id VARCHAR(18),
    measure_name VARCHAR(137),
    score VARCHAR(44),
    sample VARCHAR(15),
    footnote VARCHAR(181),
    measure_start VARCHAR(12),
    measure_end VARCHAR(12))
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES(
 "separatorChar"=",",
 "quoteChar" = '"',
 "escapeChar" = '\\'
)
STORED AS TEXTFILE
LOCATION '/user/w205/hospital_compare/raw_effective_care_dir';

DROP TABLE IF EXISTS raw_readmissions;
CREATE EXTERNAL TABLE raw_readmissions(provider_id VARCHAR(8),
   hospital_name VARCHAR(52),
   address VARCHAR(45),
   city VARCHAR(21),
   state VARCHAR(4),
   zip VARCHAR(7),
   county VARCHAR(22),
   phone VARCHAR(12),
   measure_name VARCHAR(89),
   measure_id VARCHAR(20),
   national_compare VARCHAR(37),
   denominator VARCHAR(15),
   score VARCHAR(15),
   lower_estimate VARCHAR(15),
   higher_estimate VARCHAR(15),
   footnote VARCHAR(58),
   measure_start VARCHAR(12),
   measure_end VARCHAR(12)) 
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES(
 "separatorChar"=",",
 "quoteChar" = '"',
 "escapeChar" = '\\'
)
STORED AS TEXTFILE
LOCATION '/user/w205/hospital_compare/raw_readmissions_dir';

DROP TABLE IF EXISTS raw_measures;
CREATE EXTERNAL TABLE raw_measures(measure_name VARCHAR(159),
    measure_id VARCHAR(20),
    start_quarter VARCHAR(8),
    start_date VARCHAR(21),
    end_quarter VARCHAR(8),
    end_date VARCHAR(21))
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES(
 "separatorChar"=",",
 "quoteChar" = '"',
 "escapeChar" = '\\'
)
STORED AS TEXTFILE
LOCATION '/user/w205/hospital_compare/raw_measures_dir';

DROP TABLE IF EXISTS raw_survey_responses;
CREATE EXTERNAL TABLE raw_survey_responses(provider_id VARCHAR(8),
    hospital_name VARCHAR(52),
    address VARCHAR(46),
    city VARCHAR(22),
    state VARCHAR(4),
    zip VARCHAR(12),
    county VARCHAR(22),
    comm_nurse_achieve VARCHAR(15),
    comm_nurse_improve VARCHAR(15),
    comm_nurse_dim VARCHAR(15),
    comm_doc_achieve VARCHAR(15),
    comm_doc_improve VARCHAR(15),
    comm_doc_dim VARCHAR(15),
    response_staff_achieve VARCHAR(15),
    response_staff_improve VARCHAR(15),
    response_staff_dim VARCHAR(15),
    pain_mgmt_achieve VARCHAR(15),
    pain_mgmt_improve VARCHAR(15),
    pain_mgmt_dim VARCHAR(15),
    comm_med_achieve VARCHAR(15),
    comm_med_improve VARCHAR(15),
    comm_med_dim VARCHAR(15),
    clean_quiet_achieve VARCHAR(15),
    clean_quiet_improve VARCHAR(15),
    clean_quiet_dim VARCHAR(15),
    discharge_info_achieve VARCHAR(15),
    discharge_info_improve VARCHAR(15),
    discharge_info_dim VARCHAR(15),
    overall_rating_achieve VARCHAR(15),
    overall_rating_improve VARCHAR(15),
    overall_rating_dim VARCHAR(15),
    hcahps_base_score VARCHAR(15),
    hcahps_consistency_score VARCHAR(15))
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES(
 "separatorChar"=",",
 "quoteChar" = '"',
 "escapeChar" = '\\'
)
STORED AS TEXTFILE
LOCATION '/user/w205/hospital_compare/raw_survey_responses_dir';
