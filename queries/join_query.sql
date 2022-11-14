SELECT 	id_transaction,
		date_transaction,
		b.id_customer,
        c."Product",
		c."Type",
		amount_transaction,
		b.country_customer
        
        
FROM 	bigdata_transaction a 
LEFT JOIN bigdata_customer b ON a.id_customer = b.id_customer
LEFT JOIN bigdata_product c ON a.product_transaction = c."Type"
where  c."Product" IS NOT null;
