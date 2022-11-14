DROP TABLE IF EXISTS tb_order;
CREATE TABLE tb_order (
	id_transaction INT NOT NULL,
	date_transaction DATE NOT NULL,
	id_customer INT NOT NULL,
    product VARCHAR(255) NOT NULL,
	Type VARCHAR(255) NOT NULL,
    amount_transaction VARCHAR(255) NOT NULL,
	country_customer VARCHAR(255) NOT NULL
	);
-- CREATE TABLE tb_customer (
-- 	id_customer INT NOT NULL,
-- 	name_customer VARCHAR(255) NOT NULL,
-- 	birthdate_customer DATE NOT NULL,
-- 	gender_customer VARCHAR(50) NOT NULL,
-- 	country_customer VARCHAR(255)
-- 	);

