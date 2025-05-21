SET search_path = ag_catalog, "$user", public;

SELECT * FROM cypher('pglumilineage_graph', $$ 
MATCH (sp {label: "sqlpattern", sql_hash: '8ceac2546d35e6f8a2bccba875a63e42421836e92171e76de9ee33b24f238fb8'}) 
MATCH (src_col {label: "column", fqn: 'tpcds.public.catalog_returns.cr_return_quantity'})-[df:data_flow {sql_hash: '8ceac2546d35e6f8a2bccba875a63e42421836e92171e76de9ee33b24f238fb8'}]->(tgt_col {label: "column", fqn: 'tpcds.public.monthly_channel_returns_analysis_report.primary_reason_returned_quantity'}) 
MERGE (sp)-[:generates_flow]->(df) 
$$) as (result agtype);
