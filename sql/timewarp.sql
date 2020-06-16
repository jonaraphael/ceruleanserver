ALTER TABLE shgrd 
ALTER COLUMN ingestiondate 
TYPE TIMESTAMP WITH TIME ZONE 
USING to_timestamp(ingestiondate)::timestamp at time zone 'UTC';

SELECT ingestiondate FROM shgrd;