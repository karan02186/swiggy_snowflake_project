use role sysadmin;

create database sandbox;

use database sandbox;


create warehouse if not exists swg
    comment = 'This is for end to end pipelines'
    warehouse_size = 'x-small'
    auto_resume = true
    auto_suspend = 60
    enable_query_acceleration = false
    warehouse_type = 'standard'
    min_cluster_count = 1
    max_cluster_count = 1
    scaling_policy = 'standard'
    initially_suspended = true;


create schema if not exists stage_sch;
create schema if not exists clean_sch;
create schema if not exists consumption_sch;
create schema if not exists common;

use schema stage_sch;

create file format if not exists stage_sch.csv_file_format
    type = 'csv'
    compression = auto
    field_delimiter = ','
    record_delimiter = '\n'
    skip_header = 1
    field_optionally_enclosed_by = '\042'
    null_if = ('\\N');


create stage stage_sch.csv_stg
directory = (enable=true)
comment = 'This is snowflake internal stage';

create or replace tag common.pii_policy_tag
    allowed_values 'PII','PRICE','SENSITIVE','EMAIL'
    comment = 'This is the PII policy tag object';

create or replace masking policy common.pii_masking_policy 
    as (pii_text string)
    returns string->to_varchar('**PII**');

create or replace masking policy common.pii_masking_policy 
    as (email_text string)
    returns string->to_varchar('**EMAIL**');
create or replace masking policy common.pii_masking_policy 
    as (phone string)
    returns string->to_varchar('**PHONE**');

list @csv_stg/initial;

select $1,$2,$3,$4,$5,$6,$7
from @csv_stg/initial/location
(file_format => 'stage_sch.csv_file_format');
