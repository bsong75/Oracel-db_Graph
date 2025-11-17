-- Create graph user
CREATE USER graphuser IDENTIFIED BY GraphPassword123
  DEFAULT TABLESPACE users
  TEMPORARY TABLESPACE temp
  QUOTA UNLIMITED ON users;

-- Grant necessary privileges
GRANT CONNECT, RESOURCE TO graphuser;
GRANT CREATE SESSION TO graphuser;
GRANT CREATE TABLE TO graphuser;
GRANT CREATE VIEW TO graphuser;
GRANT CREATE SEQUENCE TO graphuser;
GRANT CREATE PROCEDURE TO graphuser;

-- Grant graph-specific privileges
GRANT GRAPH_DEVELOPER TO graphuser;
GRANT PGX_SESSION_CREATE TO graphuser;
GRANT PGX_SERVER_MANAGE TO graphuser;

-- Create property graph tables schema
CREATE TABLE graphuser.entities (
    entity_id VARCHAR2(100) PRIMARY KEY,
    entity_type VARCHAR2(50)
);

CREATE TABLE graphuser.countries (
    country_code VARCHAR2(10) PRIMARY KEY,
    country_name VARCHAR2(100)
);

CREATE TABLE graphuser.months (
    month_name VARCHAR2(20) PRIMARY KEY,
    month_number NUMBER(2)
);

CREATE TABLE graphuser.target_proxies (
    target_value NUMBER(1) PRIMARY KEY,
    target_label VARCHAR2(20)
);

CREATE TABLE graphuser.country_months (
    cm_id VARCHAR2(50) PRIMARY KEY,
    country_code VARCHAR2(10),
    month_name VARCHAR2(20),
    CONSTRAINT fk_cm_country FOREIGN KEY (country_code) REFERENCES graphuser.countries(country_code),
    CONSTRAINT fk_cm_month FOREIGN KEY (month_name) REFERENCES graphuser.months(month_name)
);

CREATE TABLE graphuser.inspections (
    inspection_id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    entity_id VARCHAR2(100),
    country_code VARCHAR2(10),
    month_name VARCHAR2(20),
    target_proxy NUMBER(1),
    exams_30d NUMBER,
    pests_30d NUMBER,
    exams_90d NUMBER,
    pests_90d NUMBER,
    exams_1yr NUMBER,
    pests_1yr NUMBER,
    has_pest NUMBER(1),
    CONSTRAINT fk_insp_entity FOREIGN KEY (entity_id) REFERENCES graphuser.entities(entity_id),
    CONSTRAINT fk_insp_country FOREIGN KEY (country_code) REFERENCES graphuser.countries(country_code),
    CONSTRAINT fk_insp_month FOREIGN KEY (month_name) REFERENCES graphuser.months(month_name),
    CONSTRAINT fk_insp_target FOREIGN KEY (target_proxy) REFERENCES graphuser.target_proxies(target_value)
);

-- Create edge tables for property graph
CREATE TABLE graphuser.shipped_in_edges (
    edge_id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    entity_id VARCHAR2(100),
    month_name VARCHAR2(20),
    CONSTRAINT fk_ship_entity FOREIGN KEY (entity_id) REFERENCES graphuser.entities(entity_id),
    CONSTRAINT fk_ship_month FOREIGN KEY (month_name) REFERENCES graphuser.months(month_name)
);

CREATE TABLE graphuser.is_from_edges (
    edge_id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    entity_id VARCHAR2(100),
    country_code VARCHAR2(10),
    CONSTRAINT fk_from_entity FOREIGN KEY (entity_id) REFERENCES graphuser.entities(entity_id),
    CONSTRAINT fk_from_country FOREIGN KEY (country_code) REFERENCES graphuser.countries(country_code)
);

CREATE TABLE graphuser.has_weather_edges (
    edge_id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    entity_id VARCHAR2(100),
    cm_id VARCHAR2(50),
    CONSTRAINT fk_weather_entity FOREIGN KEY (entity_id) REFERENCES graphuser.entities(entity_id),
    CONSTRAINT fk_weather_cm FOREIGN KEY (cm_id) REFERENCES graphuser.country_months(cm_id)
);

CREATE TABLE graphuser.has_inspection_result_edges (
    edge_id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    entity_id VARCHAR2(100),
    target_value NUMBER(1),
    CONSTRAINT fk_result_entity FOREIGN KEY (entity_id) REFERENCES graphuser.entities(entity_id),
    CONSTRAINT fk_result_target FOREIGN KEY (target_value) REFERENCES graphuser.target_proxies(target_value)
);

-- Create indexes for performance
CREATE INDEX idx_insp_entity ON graphuser.inspections(entity_id);
CREATE INDEX idx_insp_country ON graphuser.inspections(country_code);
CREATE INDEX idx_insp_month ON graphuser.inspections(month_name);
CREATE INDEX idx_ship_entity ON graphuser.shipped_in_edges(entity_id);
CREATE INDEX idx_from_entity ON graphuser.is_from_edges(entity_id);
CREATE INDEX idx_weather_entity ON graphuser.has_weather_edges(entity_id);
CREATE INDEX idx_result_entity ON graphuser.has_inspection_result_edges(entity_id);

COMMIT;
