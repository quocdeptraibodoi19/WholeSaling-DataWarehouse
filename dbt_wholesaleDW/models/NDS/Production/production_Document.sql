{{ config(materialized='view') }}

select 
    documentnode,
    documentlevel,
    title,
    owner,
    folderflag,
    filename,
    fileextension,
    revision,
    changenumber,
    status,
    documentsummary,
    document,
    modifieddate,
    is_deleted,
    extract_date
from {{ source("production", "product_management_platform_document") }}