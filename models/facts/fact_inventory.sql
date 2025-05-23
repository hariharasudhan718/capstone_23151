{{
  config(
    materialized = 'incremental',
    schema = 'gold_layer'
  )
}}
SELECT 
    md5(dp.product_key) AS inventory_key,
    dp.product_key AS product_key,
    st.store_key AS store_key,
    su.supplier_key AS supplier_key,
    p.stock_quantity AS beginning_inventory,
    SUM(o.quantity) AS PurchasedQuantity,
    SUM(o.quantity) AS SoldQuantity,
    p.stock_quantity - SUM(o.quantity) AS ending_inventory,
    (p.stock_quantity * p.unit_price) AS InventoryValue,
    (SUM(o.quantity) / NULLIF((p.stock_quantity + (p.stock_quantity - SUM(o.quantity))) / 2, 0)) AS StockTurnoverRatio
FROM 
    silver_layer.order_transformations o
JOIN
    silver_layer_gold_layer.dim_product dp ON dp.product_id = o.product_id
JOIN
    silver_layer_gold_layer.dim_store st ON st.store_id = o.store_id
JOIN 
    silver_layer.product_transformations p ON p.product_id = o.product_id
JOIN 
    silver_layer_gold_layer.dim_supplier su ON su.supplier_id = p.supplier_id
GROUP BY
    dp.product_key, st.store_key, su.supplier_key, p.stock_quantity, p.unit_price
