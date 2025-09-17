use role sysadmin;
use database sandbox;
use warehouse swg;
use schema stage_sch;

select 
        t.$1::text as locationid,
        t.$2::text as city,
        t.$3::text as state,
        t.$4::text as zipcode,
        t.$5::text as activeflag,
        t.$6::text as createddate,
        t.$7::text as modifieddate,
        -- audit columns
        metadata$filename as _stg_file_name,
        metadata$file_last_modified as _stg_file_load_ts,
        metadata$file_content_key as _stg_file_md5,
        current_timestamp as _copy_data_ts
    from @stage_sch.csv_stg/initial/location 
    (file_format => 'stage_sch.csv_file_format') t;

create table stage_sch.location(
    locationid text,
    city text,
    state text,
    zipcode text,
    activeflag text,
    createddate text,
    modifieddate text,
    -- audit columns for tracking & debugging
    _stg_file_name text,
    _stg_file_load_ts timestamp,
    _stg_file_md5 text,
    _copy_data_ts timestamp default current_timestamp
);

create stream stage_sch.location_stm
on table stage_sch.location
append_only=true
comment = 'This is the append only stream object to get the data from the delta folder to the location table';

select * from stage_sch.location;
select * from stage_sch.location_stm;


copy into stage_sch.location (locationid, city, state, zipcode, activeflag, 
                    createddate, modifieddate, _stg_file_name, 
                    _stg_file_load_ts, _stg_file_md5, _copy_data_ts)
from (
    select 
        t.$1::text as locationid,
        t.$2::text as city,
        t.$3::text as state,
        t.$4::text as zipcode,
        t.$5::text as activeflag,
        t.$6::text as createddate,
        t.$7::text as modifieddate,
        metadata$filename as _stg_file_name,
        metadata$file_last_modified as _stg_file_load_ts,
        metadata$file_content_key as _stg_file_md5,
        current_timestamp as _copy_data_ts
    from @stage_sch.csv_stg/initial/location t
)
file_format = (format_name = 'stage_sch.csv_file_format')
on_error = abort_statement;



-- Level 2(moving to next schmea)
--the tables in this schema gets populated from the cdc (strams of the stage_sch) 

-- here some contraints and transformations are being made ensuring notnull values,unique values etc

use schema clean_sch;

create or replace table clean_sch.restaurant_location (
    restaurant_location_sk number autoincrement primary key,
    location_id number not null unique,
    city string(100) not null,
    state string(100) not null,
    state_code string(2) not null,
    is_union_territory boolean not null default false,
    capital_city_flag boolean not null default false,
    city_tier text(6),
    zip_code string(10) not null,
    active_flag string(10) not null,
    created_ts timestamp_tz not null,
    modified_ts timestamp_tz,
    
    -- additional audit columns
    _stg_file_name string,
    _stg_file_load_ts timestamp_ntz,
    _stg_file_md5 string,
    _copy_data_ts timestamp_ntz default current_timestamp
)
comment = 'Location entity under clean schema with appropriate data type under clean schema layer, data is populated using merge statement from the stage layer location table. This table does not support SCD2';

create or replace stream clean_sch.restaurant_location_stm
on table clean_sch.restaurant_location
comment = 'this is a standard stream object on the location table to track insert, update, and delete changes';

MERGE INTO CLEAN_SCH.RESTAURANT_LOCATION AS TARGET
USING(
    SELECT 
    CAST(LOCATIONID AS NUMBER) AS LOCATION_ID,
    CAST(CITY AS STRING) AS CITY,
    CAST(STATE AS STRING ) AS STATE,
    
    -- STATE AND STATE-CODE MAPPING
    CASE 
    WHEN State = 'Andhra Pradesh' THEN 'AP'
    WHEN State = 'Arunachal Pradesh' THEN 'AR'
    WHEN State = 'Assam' THEN 'AS'
    WHEN State = 'Bihar' THEN 'BR'
    WHEN State = 'Chhattisgarh' THEN 'CG'
    WHEN State = 'Goa' THEN 'GA'
    WHEN State = 'Gujarat' THEN 'GJ'
    WHEN State = 'Haryana' THEN 'HR'
    WHEN State = 'Himachal Pradesh' THEN 'HP'
    WHEN State = 'Jharkhand' THEN 'JH'
    WHEN State = 'Karnataka' THEN 'KA'
    WHEN State = 'Kerala' THEN 'KL'
    WHEN State = 'Madhya Pradesh' THEN 'MP'
    WHEN State = 'Maharashtra' THEN 'MH'
    WHEN State = 'Manipur' THEN 'MN'
    WHEN State = 'Meghalaya' THEN 'ML'
    WHEN State = 'Mizoram' THEN 'MZ'
    WHEN State = 'Nagaland' THEN 'NL'
    WHEN State = 'Odisha' THEN 'OR'
    WHEN State = 'Punjab' THEN 'PB'
    WHEN State = 'Rajasthan' THEN 'RJ'
    WHEN State = 'Sikkim' THEN 'SK'
    WHEN State = 'Tamil Nadu' THEN 'TN'
    WHEN State = 'Telangana' THEN 'TG'
    WHEN State = 'Tripura' THEN 'TR'
    WHEN State = 'Uttar Pradesh' THEN 'UP'
    WHEN State = 'Uttarakhand' THEN 'UK'
    WHEN State = 'West Bengal' THEN 'WB'
    
    -- Union Territories
    WHEN State = 'Andaman and Nicobar Islands' THEN 'AN'
    WHEN State = 'Chandigarh' THEN 'CH'
    WHEN State = 'Dadra and Nagar Haveli and Daman and Diu' THEN 'DN'
    WHEN State = 'Delhi' THEN 'DL'
    WHEN State = 'Jammu and Kashmir' THEN 'JK'
    WHEN State = 'Ladakh' THEN 'LA'
    WHEN State = 'Lakshadweep' THEN 'LD'
    WHEN State = 'Puducherry' THEN 'PY'
    END AS STATE_CODE,
    -- is unioun teritory
    CASE 
        WHEN STATE IN ('Andaman and Nicobar Islands','Chandigarh','Dadra and Nagar Haveli and Daman and Diu','Delhi',
        'Jammu and Kashmir','Ladakh', 'Lakshadweep','Puducherry','New Delhi')
        THEN 'Y'
        ELSE 'N'
    END AS is_union_territory,
    CASE
        
        WHEN (State = 'Andhra Pradesh' AND City = 'Amaravati') THEN TRUE
        WHEN (State = 'Arunachal Pradesh' AND City = 'Itanagar') THEN TRUE
        WHEN (State = 'Assam' AND City = 'Dispur') THEN TRUE
        WHEN (State = 'Bihar' AND City = 'Patna') THEN TRUE
        WHEN (State = 'Chhattisgarh' AND City = 'Naya Raipur') THEN TRUE
        WHEN (State = 'Goa' AND City = 'Panaji') THEN TRUE
        WHEN (State = 'Gujarat' AND City = 'Gandhinagar') THEN TRUE
        WHEN (State = 'Haryana' AND City = 'Chandigarh') THEN TRUE
        WHEN (State = 'Himachal Pradesh' AND City = 'Shimla') THEN TRUE
        WHEN (State = 'Jharkhand' AND City = 'Ranchi') THEN TRUE
        WHEN (State = 'Karnataka' AND City = 'Bangalore') THEN TRUE
        WHEN (State = 'Kerala' AND City = 'Thiruvananthapuram') THEN TRUE
        WHEN (State = 'Madhya Pradesh' AND City = 'Bhopal') THEN TRUE
        WHEN (State = 'Maharashtra' AND City = 'Mumbai') THEN TRUE
        WHEN (State = 'Manipur' AND City = 'Imphal') THEN TRUE
        WHEN (State = 'Meghalaya' AND City = 'Shillong') THEN TRUE
        WHEN (State = 'Mizoram' AND City = 'Aizawl') THEN TRUE
        WHEN (State = 'Nagaland' AND City = 'Kohima') THEN TRUE
        WHEN (State = 'Odisha' AND City = 'Bhubaneswar') THEN TRUE
        WHEN (State = 'Punjab' AND City = 'Chandigarh') THEN TRUE
        WHEN (State = 'Rajasthan' AND City = 'Jaipur') THEN TRUE
        WHEN (State = 'Sikkim' AND City = 'Gangtok') THEN TRUE
        WHEN (State = 'Tamil Nadu' AND City = 'Chennai') THEN TRUE
        WHEN (State = 'Telangana' AND City = 'Hyderabad') THEN TRUE
        WHEN (State = 'Tripura' AND City = 'Agartala') THEN TRUE
        WHEN (State = 'Uttar Pradesh' AND City = 'Lucknow') THEN TRUE
        WHEN (State = 'Uttarakhand' AND City = 'Dehradun') THEN TRUE
        WHEN (State = 'West Bengal' AND City = 'Kolkata') THEN TRUE
    
        -- Union Territories
        WHEN (State = 'Andaman & Nicobar Islands' AND City = 'Sri Vijaya Puram') THEN TRUE
        WHEN (State = 'Chandigarh' AND City = 'Chandigarh') THEN TRUE
        WHEN (State = 'Dadra & Nagar Haveli and Daman & Diu' AND City = 'Daman') THEN TRUE
        WHEN (State = 'Delhi' AND City = 'New Delhi') THEN TRUE
        WHEN (State = 'Jammu and Kashmir' AND (City = 'Srinagar' OR City = 'Jammu')) THEN TRUE
        WHEN (State = 'Ladakh' AND City = 'Leh') THEN TRUE
        WHEN (State = 'Lakshadweep' AND City = 'Kavaratti') THEN TRUE
        WHEN (State = 'Puducherry' AND City = 'Puducherry') THEN TRUE
        ELSE FALSE
    END AS capital_city_flag,
    CASE
        WHEN CITY IN ('Mumbai', 'Delhi', 'Bengaluru', 'Hyderabad', 'Chennai', 'Kolkata', 'Pune', 'Ahmedabad') THEN 
        'TIER-1'
         WHEN City IN ('Jaipur', 'Lucknow', 'Kanpur', 'Nagpur', 'Indore', 'Bhopal', 'Patna', 'Vadodara',                     'Coimbatore','Ludhiana', 'Agra', 'Nashik', 'Ranchi', 'Meerut', 'Raipur', 'Guwahati', 'Chandigarh') 
         THEN 'Tier-2'
         ELSE 'Tier-3'
    END AS CITY_TIER,
    CAST(ZipCode AS STRING) AS Zip_Code,
    CAST(ActiveFlag AS STRING) AS Active_Flag,
    TO_TIMESTAMP_TZ(CreatedDate, 'YYYY-MM-DD HH24:MI:SS') AS created_ts,
    TO_TIMESTAMP_TZ(ModifiedDate, 'YYYY-MM-DD HH24:MI:SS') AS modified_ts,
    _stg_file_name,
    _stg_file_load_ts,
    _stg_file_md5,
    CURRENT_TIMESTAMP AS _copy_data_ts
    FROM STAGE_SCH.LOCATION_STM
) AS SOURCE
ON target.location_id=source.location_id
WHEN MATCHED AND(
    target.CITY!=SOURCE.CITY OR
    target.STATE!=SOURCE.STATE OR
    target.state_code != source.state_code OR
    target.is_union_territory != source.is_union_territory OR
    target.capital_city_flag != source.capital_city_flag OR
    target.city_tier != source.city_tier OR
    target.Zip_Code != source.Zip_Code OR
    target.Active_Flag != source.Active_Flag OR
    target.modified_ts != source.modified_ts
)
THEN
    UPDATE SET 
    target.City = source.City,
        target.State = source.State,
        target.state_code = source.state_code,
        target.is_union_territory = source.is_union_territory,
        target.capital_city_flag = source.capital_city_flag,
        target.city_tier = source.city_tier,
        target.Zip_Code = source.Zip_Code,
        target.Active_Flag = source.Active_Flag,
        target.modified_ts = source.modified_ts,
        target._stg_file_name = source._stg_file_name,
        target._stg_file_load_ts = source._stg_file_load_ts,
        target._stg_file_md5 = source._stg_file_md5,
        target._copy_data_ts = source._copy_data_ts
WHEN NOT MATCHED THEN
INSERT(
    Location_ID,
        City,
        State,
        state_code,
        is_union_territory,
        capital_city_flag,
        city_tier,
        Zip_Code,
        Active_Flag,
        created_ts,
        modified_ts,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        _copy_data_ts
)
VALUES(
    source.Location_ID,
        source.City,
        source.State,
        source.state_code,
        source.is_union_territory,
        source.capital_city_flag,
        source.city_tier,
        source.Zip_Code,
        source.Active_Flag,
        source.created_ts,
        source.modified_ts,
        source._stg_file_name,
        source._stg_file_load_ts,
        source._stg_file_md5,
        source._copy_data_ts
);

SELECT * FROM clean_sch.restaurant_location_stm;
SELECT * FROM clean_sch.restaurant_location;

create or replace table consumption_sch.restaurant_location_dim (
    restaurant_location_hk NUMBER primary key,                      -- hash key for the dimension
    location_id number(38,0) not null,                  -- business key
    city varchar(100) not null,                         -- city
    state varchar(100) not null,                        -- state
    state_code varchar(2) not null,                     -- state code
    is_union_territory boolean not null default false,   -- union territory flag
    capital_city_flag boolean not null default false,     -- capital city flag
    city_tier varchar(6),                               -- city tier
    zip_code varchar(10) not null,                      -- zip code
    active_flag varchar(10) not null,                   -- active flag (indicating current record)
    eff_start_dt timestamp_tz(9) not null,              -- effective start date for scd2
    eff_end_dt timestamp_tz(9),                         -- effective end date for scd2
    current_flag boolean not null default true         -- indicator of the current record
)
comment = 'Dimension table for restaurant location with scd2 (slowly changing dimension) enabled and hashkey as surrogate key';


select * from CONSUMPTION_SCH.RESTAURANT_LOCATION_DIM;
MERGE INTO 
        CONSUMPTION_SCH.RESTAURANT_LOCATION_DIM AS target
    USING 
        CLEAN_SCH.RESTAURANT_LOCATION_STM AS source
    ON 
        target.LOCATION_ID = source.LOCATION_ID and 
        target.ACTIVE_FLAG = source.ACTIVE_FLAG
    WHEN MATCHED 
        AND source.METADATA$ACTION = 'DELETE' and source.METADATA$ISUPDATE = 'TRUE' THEN
    -- Update the existing record to close its validity period
    UPDATE SET 
        target.EFF_END_DT = CURRENT_TIMESTAMP(),
        target.CURRENT_FLAG = FALSE
    WHEN NOT MATCHED 
        AND source.METADATA$ACTION = 'INSERT' and source.METADATA$ISUPDATE = 'TRUE'
    THEN
    -- Insert new record with current data and new effective start date
    INSERT (
        RESTAURANT_LOCATION_HK,
        LOCATION_ID,
        CITY,
        STATE,
        STATE_CODE,
        IS_UNION_TERRITORY,
        CAPITAL_CITY_FLAG,
        CITY_TIER,
        ZIP_CODE,
        ACTIVE_FLAG,
        EFF_START_DT,
        EFF_END_DT,
        CURRENT_FLAG
    )
    VALUES (
        hash(SHA1_hex(CONCAT(source.CITY, source.STATE, source.STATE_CODE, source.ZIP_CODE))),
        source.LOCATION_ID,
        source.CITY,
        source.STATE,
        source.STATE_CODE,
        source.IS_UNION_TERRITORY,
        source.CAPITAL_CITY_FLAG,
        source.CITY_TIER,
        source.ZIP_CODE,
        source.ACTIVE_FLAG,
        CURRENT_TIMESTAMP(),
        NULL,
        TRUE
    )
    WHEN NOT MATCHED AND 
    source.METADATA$ACTION = 'INSERT' and source.METADATA$ISUPDATE = 'FALSE' THEN
    -- Insert new record with current data and new effective start date
    INSERT (
        RESTAURANT_LOCATION_HK,
        LOCATION_ID,
        CITY,
        STATE,
        STATE_CODE,
        IS_UNION_TERRITORY,
        CAPITAL_CITY_FLAG,
        CITY_TIER,
        ZIP_CODE,
        ACTIVE_FLAG,
        EFF_START_DT,
        EFF_END_DT,
        CURRENT_FLAG
    )
    VALUES (
        hash(SHA1_hex(CONCAT(source.CITY, source.STATE, source.STATE_CODE, source.ZIP_CODE))),
        source.LOCATION_ID,
        source.CITY,
        source.STATE,
        source.STATE_CODE,
        source.IS_UNION_TERRITORY,
        source.CAPITAL_CITY_FLAG,
        source.CITY_TIER,
        source.ZIP_CODE,
        source.ACTIVE_FLAG,
        CURRENT_TIMESTAMP(),
        NULL,
        TRUE
    );

SELECT * FROM CONSUMPTION_SCH.RESTAURANT_LOCATION_DIM;

-- Table-level upstream lineage
SELECT *
FROM TABLE(SNOWFLAKE.CORE.GET_LINEAGE('SANDBOX.CLEAN_SCH.RESTAURANT_LOCATION', 'TABLE', 'DOWNSTREAM'));

SHOW TABLES IN SCHEMA CLEAN_SCH;
SHOW GRANTS ON TABLE CLEAN_SCH.RESTAURANT_LOCATION;

SELECT * FROM "CLEAN_SCH"."RESTAURANT_LOCATION";

-- Column-level upstream lineage
SELECT *
FROM TABLE(SNOWFLAKE.CORE.GET_LINEAGE_COLUMNS('CLEAN_SCH.RESTAURANT_LOCATION', 'TABLE', 'UPSTREAM'));

--------------  PART 2 , NEW FILE LOADING AFTER THE INITIAL LOAD(CAN SAY INCREMENTAL LOAD)

LIST @SANDBOX.STAGE_SCH.CSV_STG/delta/location/;

copy into stage_sch.location (locationid, city, state, zipcode, activeflag, 
                    createddate, modifieddate, _stg_file_name, 
                    _stg_file_load_ts, _stg_file_md5, _copy_data_ts)
from (
    select 
        t.$1::text as locationid,
        t.$2::text as city,
        t.$3::text as state,
        t.$4::text as zipcode,
        t.$5::text as activeflag,
        t.$6::text as createddate,
        t.$7::text as modifieddate,
        metadata$filename as _stg_file_name,
        metadata$file_last_modified as _stg_file_load_ts,
        metadata$file_content_key as _stg_file_md5,
        current_timestamp as _copy_data_ts
    from @stage_sch.csv_stg/delta/location/delta-day02-2rows-update.csv t
)
file_format = (format_name = 'stage_sch.csv_file_format')
on_error = abort_statement;
-- 7 records are copied in initial table
select * from stage_sch.location ;

select * from SANDBOX.STAGE_SCH.LOCATION_STM;

-- now run the merge stmt (sstm1 to rest_location table)
select * from clean_sch.restaurant_location;
--3 insert and 4 update

select * from clean_sch.restaurant_location_stm;
---this above que shows 11 records
-- 4+4 for update(del and insert ) and 3 for new records

-- run the merge stmt btw the stm2 and location dim table
-- this shows 4 inserted and 4 updated
select * from CONSUMPTION_SCH.RESTAURANT_LOCATION_DIM;
---





