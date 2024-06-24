--return difference in table contents on specific ID.
SELECT old.* ,new.*
  FROM PLANES BEFORE(STATEMENT => '01b4fac9-0302-bea8-0002-c4be0002112e') AS old
    FULL OUTER JOIN PLANES AT(STATEMENT => '01b4fac9-0302-bea8-0002-c4be0002112e') AS new
    ON old.id = new.id
  WHERE old.id IS NULL OR new.id IS NULL;

--return table contents 1 minute ago.
SELECT * FROM PLANES AT(offset => -60);

--create with table contests of Plane tables committed 5 minutes ago.
CREATE TABLE restored_table CLONE PLANES AT (OFFSET => -3600);

--create database with all object in AIRLINES database existed 5 days ago.
CREATE DATABASE RESTORED_AIRLINES CLONE RESTORED_AIRLINES
  AT(TIMESTAMP => DATEADD(days, -5, current_timestamp)::timestamp_tz)
  IGNORE TABLES WITH INSUFFICIENT DATA RETENTION;