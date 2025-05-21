SET search_path = ag_catalog, "$user", public;

SELECT * FROM cypher('pglumilineage_graph', $$ 
// 步骤 1: 匹配源节点和目标节点
MATCH (src_col {label: "column", fqn: 'tpcds.public.date_dim.d_date'})
MATCH (tgt_col {label: "column", fqn: 'tpcds.public.monthly_channel_returns_analysis_report.sales_year_month'})

// 步骤 2: MERGE 关系，仅包含用于唯一标识和匹配的属性
MERGE (src_col)-[df:data_flow {sql_hash: '8ceac2546d35e6f8a2bccba875a63e42421836e92171e76de9ee33b24f238fb8'}]->(tgt_col)

// 步骤 3: 设置/更新那些每次都应该更新的属性
SET df.transformation_logic = 'TO_CHAR(d.d_date, \'YYYY-MM\')',
    df.derivation_type = 'FUNCTION_CALL',
    df.last_seen_at = '2025-05-20 17:04:47'

// 步骤 4: 条件性地设置 created_at
SET df.created_at = COALESCE(df.created_at, '2025-05-20 17:04:47')

// 步骤 5: 返回一些信息以确认操作
RETURN id(src_col) AS src_id, id(df) AS df_id, id(tgt_col) AS tgt_id
$$) as (src_id agtype, df_id agtype, tgt_id agtype);
