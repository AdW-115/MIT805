-- create item_metadata table to which jsonl item metadata will be exported

DROP TABLE IF EXISTS item_metadata

CREATE TABLE ITEM_METADATA (
	parent_asin VARCHAR(255) PRIMARY KEY,
	main_category VARCHAR(255),
	brand TEXT,
	average_rating REAL,
	rating_number INT,
	price VARCHAR(255),
	store_name VARCHAR(255),
	granular_category TEXT
);