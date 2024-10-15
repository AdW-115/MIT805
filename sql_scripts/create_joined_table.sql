-- join the item_reviews and item_metadata table on the parent_asin field to be able to do time series analysis on different aspects
-- convert the unix timestamp to a human readable format and extract the month and year value
-- store these results in a table called review_month_year

CREATE TABLE review_month_year AS (
	SELECT item_reviews.parent_asin, EXTRACT(MONTH FROM (to_timestamp(item_reviews.time_stamp/1000)::date)) AS MONTH, 
	EXTRACT(YEAR FROM (to_timestamp(item_reviews.time_stamp/1000)::date)) AS YEAR, item_metadata.brand, item_metadata.granular_category
	FROM item_reviews
	JOIN item_metadata ON item_reviews.parent_asin = item_metadata.parent_asin
);