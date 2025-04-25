
with renamed as (

    select
        id,
        lower(function_name) as function_name,
        page_name,
        url,
        categories as category_array,
        categories[1] AS category,
        description 
    from {{ source('raw_sf', 'sf_sql_func_dict_all') }}


),

finale as (

select
    id,
    case 
        when function_name = 'is  null' then 'is null'
        when function_name like ('%snowflake.core%') then REGEXP_REPLACE(function_name, ' \(snowflake.core\)', '')
        when function_name = 'md5_number — obsoleted' then 'md5_number'
        else function_name end as function_name,
    page_name,
    url,
    category_array,
    case 
        when function_name = 'lag' then 'Window function syntax and usage'
        when function_name = 'parse_json' then 'Semi-structured and structured data functions'
        when function_name = 'generate_column_description' then 'Metadata functions'
        when function_name = 'jarowinkler_similarity' then 'String & binary functions'
        when function_name = 'map_cat' then 'Semi-structured and structured data functions'
        when function_name = 'editdistance' then 'String & binary functions'
        when function_name = 'uuid_string' then 'String & binary functions'
        when function_name = 'normal' then 'Data generation functions'
        when function_name = 'randstr' then 'Data generation functions'
        when function_name = 'system$remove_reference' then 'System functions'
        when function_name = 'h3_cell_to_boundary' then 'Geospatial functions'
        when function_name = 'tag_references_with_lineage' then 'Account Usage table functions'
        when function_name = 'system$validate_storage_integration' then 'System functions'
        when function_name = 'object_construct_keep_null' then 'Semi-structured and structured data functions'
        when function_name = 'iceberg_table_files' then 'Table functions'
        when function_name = 'system$get_resultset_status' then 'System functions'
        when function_name = 'zipf' then 'Data generation functions'
        when function_name = 'search' then 'String & binary functions'
        when function_name = 'var_samp' then 'Aggregate functions'
        when function_name = 'kurtosis' then 'Aggregate functions'
        when function_name = 'square' then 'Numeric functions'
        when function_name = 'factorial' then 'Numeric functions'
        when function_name = 'sentiment' then 'String & binary functions'
        else category
    end as category,
    description
from renamed
where description != '' and function_name not in('to_timestamp_','is_timestamp_','as_<object_type>','query_history_by_', 'as_timestamp_', 'is_<object_type>', 'try_to_timestamp_')

), 

unionall as (

select * from finale
    union all
    select 
        0 as id,
        'no_function_found' as function_name,
        'no_function_found' as page_name,
        null as url,
        null as category_array,
        'no_function_found' as category,
        -- null as category_2,
        -- null as category_3,
        -- null as subcategory,
        'no_function_found' as description
    union all
    select 
        0 as id,
        'is not null' as function_name,
        'is-null' as page_name,
        'https://docs.snowflake.com/en/sql-reference/functions/is-null' as url,
        null as category_array,
        'Conditional expression functions' as category,
        -- null as category_2,
        -- null as category_3,
        -- null as subcategory,
        'Determines whether an expression is NULL or is not NULL.' as description
    union all
select 
    0 as id,
    'to_timestamp_ltz' as function_name,
    'to_timestamp' as page_name,
    'https://docs.snowflake.com/en/sql-reference/functions/to_timestamp' as url,
    null as category_array,
    'conversion functions' as category,
    -- null as category_2,
    -- null as category_3,
    -- null as subcategory,
    'converts an input expression into the corresponding timestamp' as description

union all
select 
    0 as id,
    'to_timestamp_ntz' as function_name,
    'to_timestamp' as page_name,
    'https://docs.snowflake.com/en/sql-reference/functions/to_timestamp' as url,
    null as category_array,
    'conversion functions' as category,
    -- null as category_2,
    -- null as category_3,
    -- null as subcategory,
    'converts an input expression into the corresponding timestamp' as description

union all
select 
    0 as id,
    'to_timestamp_tz' as function_name,
    'to_timestamp' as page_name,
    'https://docs.snowflake.com/en/sql-reference/functions/to_timestamp' as url,
    null as category_array,
    'conversion functions' as category,
    -- null as category_2,
    -- null as category_3,
    -- null as subcategory,
    'converts an input expression into the corresponding timestamp' as description

union all
select 
    0 as id,
    'is_timestamp_ltz' as function_name,
    'is_timestamp' as page_name,
    'https://docs.snowflake.com/en/sql-reference/functions/is_timestamp' as url,
    null as category_array,
    'semi-structured and structured data functions' as category,
    -- null as category_2,
    -- null as category_3,
    -- null as subcategory,
    'verifies whether a variant argument contains the respective timestamp value' as description

union all
select 
    0 as id,
    'is_timestamp_ntz' as function_name,
    'is_timestamp' as page_name,
    'https://docs.snowflake.com/en/sql-reference/functions/is_timestamp' as url,
    null as category_array,
    'semi-structured and structured data functions' as category,
    -- null as category_2,
    -- null as category_3,
    -- null as subcategory,
    'verifies whether a variant argument contains the respective timestamp value' as description

union all
select 
    0 as id,
    'is_timestamp_tz' as function_name,
    'is_timestamp' as page_name,
    'https://docs.snowflake.com/en/sql-reference/functions/is_timestamp' as url,
    null as category_array,
    'semi-structured and structured data functions' as category,
    -- null as category_2,
    -- null as category_3,
    -- null as subcategory,
    'verifies whether a variant argument contains the respective timestamp value' as description

union all
select 
    0 as id,
    'query_history_by_session' as function_name,
    null as page_name,
    'https://docs.snowflake.com/en/sql-reference/functions/query_history' as url,
    null as category_array,
    'information schema' as category,
    -- null as category_2,
    -- null as category_3,
    -- null as subcategory,
    'the query_history family of table functions can be used to query snowflake query history along various dimensions' as description

union all
select 
    0 as id,
    'query_history_by_user' as function_name,
    null as page_name,
    'https://docs.snowflake.com/en/sql-reference/functions/query_history' as url,
    null as category_array,
    'information schema' as category,
    -- null as category_2,
    -- null as category_3,
    -- null as subcategory,
    'the query_history family of table functions can be used to query snowflake query history along various dimensions' as description

union all
select 
    0 as id,
    'query_history_by_warehouse' as function_name,
    null as page_name,
    'https://docs.snowflake.com/en/sql-reference/functions/query_history' as url,
    null as category_array,
    'information schema' as category,
    -- null as category_2,
    -- null as category_3,
    -- null as subcategory,
    'the query_history family of table functions can be used to query snowflake query history along various dimensions' as description

union all
select 
    0 as id,
    'as_timestamp_ltz' as function_name,
    'as_timestamp' as page_name,
    'https://docs.snowflake.com/en/sql-reference/functions/as_timestamp' as url,
    null as category_array,
    'semi-structured and structured data functions' as category,
    -- null as category_2,
    -- null as category_3,
    -- null as subcategory,
    'casts a variant value to the respective timestamp value' as description

union all
select 
    0 as id,
    'as_timestamp_ntz' as function_name,
    'as_timestamp' as page_name,
    'https://docs.snowflake.com/en/sql-reference/functions/as_timestamp' as url,
    null as category_array,
    'semi-structured and structured data functions' as category,
    -- null as category_2,
    -- null as category_3,
    -- null as subcategory,
    'casts a variant value to the respective timestamp value' as description

union all
select 
    0 as id,
    'as_timestamp_tz' as function_name,
    'as_timestamp' as page_name,
    'https://docs.snowflake.com/en/sql-reference/functions/as_timestamp' as url,
    null as category_array,
    'semi-structured and structured data functions' as category,
    -- null as category_2,
    -- null as category_3,
    -- null as subcategory,
    'casts a variant value to the respective timestamp value' as description
union all
select 
    0 as id,
    'try_to_timestamp_ltz' as function_name,
    'try_to_timestamp' as page_name,
    'https://docs.snowflake.com/en/sql-reference/functions/try_to_timestamp' as url,
    null as category_array,
    'Conversion functions' as category,
    -- null as category_2,
    -- null as category_3,
    -- null as subcategory,
    'casts a variant value to the respective timestamp value' as description

union all
select 
    0 as id,
    'try_to_timestamp_ntz' as function_name,
    'try_to_timestamp' as page_name,
    'https://docs.snowflake.com/en/sql-reference/functions/try_to_timestamp' as url,
    null as category_array,
    'Conversion functions' as category,
    -- null as category_2,
    -- null as category_3,
    -- null as subcategory,
    'casts a variant value to the respective timestamp value' as description
union all
select 
    0 as id,
    'try_to_timestamp_tz' as function_name,
    'try_to_timestamp' as page_name,
    'https://docs.snowflake.com/en/sql-reference/functions/try_to_timestamp' as url,
    null as category_array,
    'Conversion functions' as category,
    -- null as category_2,
    -- null as category_3,
    -- null as subcategory,
    'casts a variant value to the respective timestamp value' as description


)

select 
    id,
    function_name,
    page_name,
    url,
    category_array,
    category,
    description
from unionall
