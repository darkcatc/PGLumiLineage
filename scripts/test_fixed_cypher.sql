SET search_path = ag_catalog, "$user", public;

-- 测试修改后的 Cypher 语句
SELECT * FROM cypher('pglumilineage_graph', $$ 
MATCH (sp {label: "sqlpattern", sql_hash: '8ceac2546d35e6f8a2bccba875a63e42421836e92171e76de9ee33b24f238fb8'}) 
MATCH (src_col {label: "column", fqn: 'tpcds.public.catalog_returns.cr_return_quantity'})
MATCH (tgt_col {label: "column", fqn: 'tpcds.public.monthly_channel_returns_analysis_report.primary_reason_returned_quantity'})
MERGE (sp)-[:generates]->(src_col)
MERGE (sp)-[:generates]->(tgt_col)
$$) as (result agtype);
