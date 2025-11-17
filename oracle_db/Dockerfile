FROM gvenzl/oracle-free:latest

# Expose Oracle listener and EM Express ports
EXPOSE 1521 5500

ENV ORACLE_PASSWORD=OraclePassword123
ENV ORACLE_DATABASE=GRAPHDB

# The base image already has the startup script
# Container will automatically initialize the database
