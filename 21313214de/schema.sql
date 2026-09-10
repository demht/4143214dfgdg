CREATE TABLE orders (
	id INTEGER NOT NULL, 
	user_id BIGINT NOT NULL, 
	username VARCHAR(64), 
	payment_method VARCHAR(16) NOT NULL, 
	amount INTEGER NOT NULL, 
	currency VARCHAR(8) NOT NULL, 
	status VARCHAR(16) NOT NULL, 
	promo_code VARCHAR(64), 
	telegram_payment_charge_id VARCHAR(256), 
	provider_payment_charge_id VARCHAR(256), 
	created_at DATETIME NOT NULL, 
	paid_at DATETIME, 
	delivered_at DATETIME, 
	duration_seconds INTEGER, 
	PRIMARY KEY (id), 
	UNIQUE (telegram_payment_charge_id)
);
CREATE INDEX ix_orders_user_id ON orders (user_id);
CREATE INDEX ix_orders_status ON orders (status);
CREATE TABLE licenses (
	user_id BIGINT NOT NULL, 
	username VARCHAR(64), 
	source_order_id INTEGER NOT NULL, 
	purchased_at DATETIME NOT NULL, 
	active BOOLEAN NOT NULL, 
	license_id VARCHAR(32), 
	expires_at DATETIME, 
	device_hash VARCHAR(128), 
	activated_at DATETIME, 
	last_seen_at DATETIME, 
	binding_token VARCHAR(64), 
	last_version_sent VARCHAR(64), 
	last_release_sent_at DATETIME, 
	PRIMARY KEY (user_id)
);
CREATE INDEX ix_licenses_active ON licenses (active);
CREATE INDEX ix_licenses_source_order_id ON licenses (source_order_id);
CREATE TABLE releases (
	id INTEGER NOT NULL, 
	version VARCHAR(64) NOT NULL, 
	file_path VARCHAR(512) NOT NULL, 
	file_name VARCHAR(256) NOT NULL, 
	notes TEXT NOT NULL, 
	created_by BIGINT NOT NULL, 
	created_at DATETIME NOT NULL, 
	status VARCHAR(16) NOT NULL, 
	total_count INTEGER NOT NULL, 
	sent_count INTEGER NOT NULL, 
	failed_count INTEGER NOT NULL, 
	PRIMARY KEY (id)
);
CREATE INDEX ix_releases_version ON releases (version);
CREATE INDEX ix_releases_status ON releases (status);
CREATE TABLE release_deliveries (
	release_id INTEGER NOT NULL, 
	user_id BIGINT NOT NULL, 
	status VARCHAR(16) NOT NULL, 
	error TEXT, 
	sent_at DATETIME, 
	PRIMARY KEY (release_id, user_id)
);
CREATE INDEX ix_release_deliveries_status ON release_deliveries (status);
CREATE UNIQUE INDEX ux_licenses_license_id ON licenses (license_id);
