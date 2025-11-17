-- Copy data from FREE database to FREEPDB1
-- This allows Graph Studio (which connects to FREEPDB1) to see the data

-- First, ensure graphuser has privileges in FREEPDB1
-- Run this as SYS in FREEPDB1

-- Grant CREATE TABLE privilege
GRANT CREATE TABLE TO graphuser;
GRANT UNLIMITED TABLESPACE TO graphuser;

-- Create tables in FREEPDB1
CREATE TABLE entities AS SELECT * FROM graphuser.entities@FREE;
CREATE TABLE countries AS SELECT * FROM graphuser.countries@FREE;
CREATE TABLE months AS SELECT * FROM graphuser.months@FREE;
CREATE TABLE inspections AS SELECT * FROM graphuser.inspections@FREE;
CREATE TABLE target_proxies AS SELECT * FROM graphuser.target_proxies@FREE;
CREATE TABLE country_months AS SELECT * FROM graphuser.country_months@FREE;

-- Create edge tables
CREATE TABLE shipped_in_edges AS SELECT * FROM graphuser.shipped_in_edges@FREE;
CREATE TABLE is_from_edges AS SELECT * FROM graphuser.is_from_edges@FREE;
CREATE TABLE has_weather_edges AS SELECT * FROM graphuser.has_weather_edges@FREE;
CREATE TABLE has_inspection_result_edges AS SELECT * FROM graphuser.has_inspection_result_edges@FREE;

-- Verify counts
SELECT 'ENTITIES' as table_name, COUNT(*) as count FROM entities
UNION ALL
SELECT 'COUNTRIES', COUNT(*) FROM countries
UNION ALL
SELECT 'MONTHS', COUNT(*) FROM months
UNION ALL
SELECT 'INSPECTIONS', COUNT(*) FROM inspections;
