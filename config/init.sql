-- 1. Define your base path here
CREATE OR REPLACE MACRO base_path(file) AS '~/dev/crosv_project/data/' || file;

CREATE TABLE IF NOT EXISTS r_release_history (
    r_version VARCHAR PRIMARY KEY,
    nickname VARCHAR,
    release_date DATE,
    devel_version VARCHAR,
    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE OR REPLACE TEMP VIEW stage_r_info AS
WITH entries AS (
  select key, trim(value) as value
  from read_json_auto(base_path('VERSION-INFO.json'))
),
pivoted AS (
    SELECT 
        -- Trim the values as they contain leading spaces (e.g., " 4.5.3")
        trim(max(CASE WHEN entries.key = 'Release' THEN entries.value END)) AS r_version,
        trim(max(CASE WHEN entries.key = 'Nickname' THEN entries.value END)) AS nickname,
        trim(max(CASE WHEN entries.key = 'Date' THEN entries.value END))::DATE AS release_date,
        trim(max(CASE WHEN entries.key = 'Devel' THEN entries.value END)) AS devel_version
    FROM entries
)
SELECT * FROM pivoted;

-- 2. Conditional Insert: Only insert if this R version doesn't exist yet
INSERT INTO r_release_history (r_version, nickname, release_date, devel_version)
SELECT s.* FROM stage_r_info s
WHERE s.r_version NOT IN (SELECT r_version FROM r_release_history);

-- Create the Lineage Table
CREATE TABLE IF NOT EXISTS data_refresh_log (
    run_id INTEGER PRIMARY KEY,
    refresh_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    cran_packages_source VARCHAR,
    cran_archive_source VARCHAR,
    osv_source_pattern VARCHAR,
    status VARCHAR
);

-- Sequence to handle run IDs
CREATE SEQUENCE IF NOT EXISTS seq_run_id START 1;

-- Refresh script
BEGIN TRANSACTION;

-- 1. Setup Macro
CREATE OR REPLACE MACRO r_ver(v) AS 
    list_transform(str_split_regex(v, '[\.-]'), x -> try_cast(x AS INTEGER));

-- 2. Staging Data
CREATE OR REPLACE TABLE stage_cran_current AS 
SELECT * FROM read_csv(
    base_path('packages.csv'),
    auto_detect = true, delim = ';', quote = '"', header = true,
    types={'Date/Publication': 'VARCHAR', 'Published': 'VARCHAR'}
);

CREATE OR REPLACE TABLE stage_cran_archive AS 
SELECT * FROM read_csv(
    base_path('archive.csv'),
    auto_detect = true, delim = ';', quote = '"', header = true,
    types={'size': 'VARCHAR'}
);

-- 3. Flattening OSV
CREATE OR REPLACE TEMP TABLE osv_flat AS
SELECT id AS osv_id, 
    aff.package.name AS Package, 
    unnest(aff.ranges).events AS events
FROM (
    SELECT id, unnest(affected) AS aff 
    FROM read_json_auto(base_path('/ALL/*.json')))
WHERE aff.package.ecosystem = 'CRAN';

-- 4. Updating Production Lookup
CREATE OR REPLACE TABLE vulnerability_lookup AS
SELECT 
    Package, osv_id,
    (list_filter(events, x -> x.introduced IS NOT NULL)[1]).introduced AS introduced_v,
    (list_filter(events, x -> x.fixed IS NOT NULL)[1]).fixed AS fixed_v
FROM osv_flat;

-- 4.5
CREATE OR REPLACE TEMP VIEW stage_r_info AS
WITH entries AS (
  select key, trim(value) as value
  from read_json_auto(base_path('VERSION-INFO.json'))
),
pivoted AS (
    SELECT 
        -- Trim the values as they contain leading spaces (e.g., " 4.5.3")
        trim(max(CASE WHEN entries.key = 'Release' THEN entries.value END)) AS r_version,
        trim(max(CASE WHEN entries.key = 'Nickname' THEN entries.value END)) AS nickname,
        trim(max(CASE WHEN entries.key = 'Date' THEN entries.value END))::DATE AS release_date,
        trim(max(CASE WHEN entries.key = 'Devel' THEN entries.value END)) AS devel_version
    FROM entries
)
SELECT * FROM pivoted;

-- 2. Conditional Insert: Only insert if this R version doesn't exist yet
INSERT INTO r_release_history (r_version, nickname, release_date, devel_version)
SELECT s.* FROM stage_r_info s
WHERE s.r_version NOT IN (SELECT r_version FROM r_release_history);

-- 5. Log the Refresh for Compliance
INSERT INTO data_refresh_log (run_id, cran_packages_source, cran_archive_source, osv_source_pattern, status)
VALUES (
    nextval('seq_run_id'), 
    './data/packages.csv', 
    './data/archive.csv', 
    './osv_records/*.json', 
    'SUCCESS'
);

COMMIT;

-- 5. Final Safety View
CREATE OR REPLACE VIEW package_safety_report AS
WITH all_packages AS (
    SELECT 
        Package, 
        Version, 
        Title,
        Description,
        Published,
        'current' AS status 
    FROM stage_cran_current
    UNION ALL
    SELECT 
        regexp_extract(file_name, '^([^/]+)', 1) AS Package,
        regexp_extract(file_name, '_([0-9.-]+)\.tar\.gz$', 1) AS Version,
        NULL AS Title,
        NULL AS Description,
        NULL AS Published,
        'archived' AS status 
    FROM stage_cran_archive
)
SELECT 
    c.Package,
    c.Version,
    c.Title,
    c.Description,
    c.Published,
    c.status,
    v.osv_id,
    CASE 
        WHEN v.osv_id IS NULL THEN 'SAFE'
        WHEN v.fixed_v IS NOT NULL AND r_ver(c.Version) >= r_ver(v.fixed_v) THEN 'FIXED'
        WHEN v.introduced_v IS NOT NULL AND r_ver(c.Version) >= r_ver(v.introduced_v) THEN 'VULNERABLE'
        ELSE 'SAFE'
    END AS osv_safety_status
FROM all_packages c
LEFT JOIN vulnerability_lookup v ON c.Package = v.Package;

-- 6. Final Safety View
CREATE OR REPLACE VIEW packages_search AS
WITH all_packages AS (
    -- Combine current and archived packages into a single set
    SELECT 
        Package,
        Title,
        Description,
        Published,
        Version, 
        'current' AS status 
    FROM stage_cran_current
)
SELECT 
    c.Package,
    c.Title,
    c.Description,
    c.Published,
    c.Version,
    c.status,
    v.osv_id,
    CASE 
        -- 1. No matching vulnerability found
        WHEN v.osv_id IS NULL THEN 'SAFE'
        
        -- 2. Vulnerability exists, but current version is >= the fixed version
        WHEN v.fixed_v IS NOT NULL AND r_ver(c.Version) >= r_ver(v.fixed_v) THEN 'FIXED'
        
        -- 3. Current version is >= the version where the vulnerability was introduced
        WHEN v.introduced_v IS NOT NULL AND r_ver(c.Version) >= r_ver(v.introduced_v) THEN 'VULNERABLE'
        
        -- 4. Default fallback
        ELSE 'SAFE'
    END AS osv_safety_status
FROM all_packages c
LEFT JOIN vulnerability_lookup v ON c.Package = v.Package;