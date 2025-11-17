-- Create real edge tables that connect entities to entities
-- based on shared attributes (country, month)

-- Drop existing edge tables if they exist
BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE entity_same_country_edges';
EXCEPTION
   WHEN OTHERS THEN NULL;
END;
/

BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE entity_same_month_edges';
EXCEPTION
   WHEN OTHERS THEN NULL;
END;
/

-- Create edges between entities from the same country
CREATE TABLE entity_same_country_edges AS
SELECT
    ROWNUM as edge_id,
    i1.entity_id as source_entity_id,
    i2.entity_id as destination_entity_id,
    i1.country_code
FROM inspections i1
JOIN inspections i2 ON i1.country_code = i2.country_code
WHERE i1.entity_id < i2.entity_id;  -- Avoid duplicates and self-loops

-- Create edges between entities shipped in the same month
CREATE TABLE entity_same_month_edges AS
SELECT
    ROWNUM as edge_id,
    i1.entity_id as source_entity_id,
    i2.entity_id as destination_entity_id,
    i1.month_name
FROM inspections i1
JOIN inspections i2 ON i1.month_name = i2.month_name
WHERE i1.entity_id < i2.entity_id;  -- Avoid duplicates and self-loops

-- Add primary keys
ALTER TABLE entity_same_country_edges ADD CONSTRAINT pk_country_edges PRIMARY KEY (edge_id);
ALTER TABLE entity_same_month_edges ADD CONSTRAINT pk_month_edges PRIMARY KEY (edge_id);

-- Show counts
SELECT 'Same Country Edges' as edge_type, COUNT(*) as count FROM entity_same_country_edges
UNION ALL
SELECT 'Same Month Edges', COUNT(*) FROM entity_same_month_edges;
