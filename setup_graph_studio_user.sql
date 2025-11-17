-- Setup Graph Studio User
-- Run this as SYS to grant Graph privileges to graphuser

-- Connect as SYS
-- CONNECT sys/OraclePassword123@localhost:1521/FREE AS SYSDBA

-- Grant Graph-related roles and privileges
GRANT GRAPH_DEVELOPER TO graphuser;
GRANT PGX_SESSION_CREATE TO graphuser;
GRANT PGX_SERVER_GET_INFO TO graphuser;
GRANT PGX_SERVER_MANAGE TO graphuser;

-- Grant unlimited tablespace (if not already granted)
ALTER USER graphuser QUOTA UNLIMITED ON users;

-- Verify grants
SELECT * FROM dba_role_privs WHERE grantee = 'GRAPHUSER';
SELECT * FROM dba_sys_privs WHERE grantee = 'GRAPHUSER';

-- Create a simple test to verify user can access graph features
-- This should be run as graphuser
-- SELECT * FROM user_role_privs WHERE granted_role LIKE '%GRAPH%';
