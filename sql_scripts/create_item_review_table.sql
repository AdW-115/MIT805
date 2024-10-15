-- create item_reviews table to which jsonl item review data will be exported

DROP TABLE IF EXISTS item_reviews;

CREATE TABLE item_reviews (
	review_id INT GENERATED ALWAYS AS IDENTITY,
	rating REAL,
	asin VARCHAR(255),
	parent_asin VARCHAR(255),
	user_id VARCHAR(255),
	time_stamp BIGINT,
	verified_purchase BOOL,
	helpful_vote INT,
	PRIMARY KEY(review_id),
	CONSTRAINT fk_reviews
		FOREIGN KEY(parent_asin)
			REFERENCES item_metadata(parent_asin)
);