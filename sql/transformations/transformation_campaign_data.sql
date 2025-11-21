SELECT * FROM staging_campaign_data

UPDATE staging_campaign_data
SET discount = SUBSTRING(discount FROM '^[0-9]+');

ALTER TABLE staging_campaign_data
ALTER COLUMN discount TYPE NUMERIC(10,2) 
USING discount::NUMERIC / 100.0;

SELECT * FROM staging_campaign_data

-- discount is in percent, may seem less intuitive but more 
-- applicable for calculations sa BI layer