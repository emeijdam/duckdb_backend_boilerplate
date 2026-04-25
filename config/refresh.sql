-- Refresh script
BEGIN TRANSACTION;

-- 2. Staging Data
CREATE OR REPLACE TABLE stage_cran_current AS 
SELECT * FROM read_csv(
    './data/packages.csv',
    auto_detect = true, delim = ';', quote = '"', header = true,
    types={'Date/Publication': 'VARCHAR', 'Published': 'VARCHAR'}
);

CREATE OR REPLACE TABLE stage_cran_archive AS 
SELECT * FROM read_csv(
    './data/archive.csv',
    auto_detect = true, delim = ';', quote = '"', header = true,
    types={'size': 'VARCHAR'}
);

-- 3. Flattening OSV
CREATE OR REPLACE TABLE osv_flat AS
SELECT id AS osv_id, 
    aff.package.name AS Package, 
    unnest(aff.ranges).events AS events
FROM (
    SELECT id, unnest(affected) AS aff 
    FROM read_json_auto('./data/ALL/*.json'))
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
  from read_json_auto('./data/VERSION-INFO.json')
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