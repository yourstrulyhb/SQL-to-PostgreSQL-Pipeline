-- Create etl user
CREATE USER user_etl WITH PASSWORD 'demopass';
-- Grant connect permission
GRANT CONNECT ON DATABASE "AdventureWorks" TO user_etl;
-- Grant table permissions
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO user_etl;