--指标1：年度各销售渠道总销售额、总净利润和平均客单价
-- 目的：宏观了解不同销售渠道（实体店、目录、网站）的年度盈利能力和客户消费水平。
-- 报表字段：年份, 渠道名称, 总销售额 (含税和运费), 总净利润, 总订单数, 平均客单价。
-- 创建年度渠道销售与利润汇总报表
CREATE TABLE IF NOT EXISTS annual_channel_performance_report (
    report_year INTEGER,
    channel VARCHAR(20),
    total_sales_amount_inc_tax_ship NUMERIC(15, 2), -- 对应 xx_net_paid_inc_tax 或 xx_net_paid_inc_ship_tax
    total_net_profit NUMERIC(15, 2),                -- 对应 xx_net_profit
    total_orders BIGINT,
    average_transaction_value NUMERIC(15, 2)
);

-- 如果需要重复执行并刷新数据，可以先清空表
-- 不再需要TRUNCATE，因为我们已经使用DROP TABLE
TRUNCATE TABLE annual_channel_performance_report;

INSERT INTO annual_channel_performance_report (report_year, channel, total_sales_amount_inc_tax_ship, total_net_profit, total_orders, average_transaction_value)
-- 实体店销售 (Store Sales)
SELECT
    d.d_year AS report_year,
    'Store' AS channel,
    SUM(COALESCE(ss.ss_net_paid_inc_tax, 0)) AS total_sales_amount_inc_tax_ship, -- 实体店一般不单独列出发货费相关的net_paid
    SUM(COALESCE(ss.ss_net_profit, 0)) AS total_net_profit,
    COUNT(DISTINCT ss.ss_ticket_number) AS total_orders,
    CASE
        WHEN COUNT(DISTINCT ss.ss_ticket_number) > 0 THEN SUM(COALESCE(ss.ss_net_paid_inc_tax, 0)) / COUNT(DISTINCT ss.ss_ticket_number)
        ELSE 0
    END AS average_transaction_value
FROM
    store_sales ss
JOIN
    date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
WHERE
    d.d_year IS NOT NULL
GROUP BY
    d.d_year

UNION ALL

-- 目录销售 (Catalog Sales)
SELECT
    d.d_year AS report_year,
    'Catalog' AS channel,
    SUM(COALESCE(cs.cs_net_paid_inc_ship_tax, 0)) AS total_sales_amount_inc_tax_ship,
    SUM(COALESCE(cs.cs_net_profit, 0)) AS total_net_profit,
    COUNT(DISTINCT cs.cs_order_number) AS total_orders,
    CASE
        WHEN COUNT(DISTINCT cs.cs_order_number) > 0 THEN SUM(COALESCE(cs.cs_net_paid_inc_ship_tax, 0)) / COUNT(DISTINCT cs.cs_order_number)
        ELSE 0
    END AS average_transaction_value
FROM
    catalog_sales cs
JOIN
    date_dim d ON cs.cs_sold_date_sk = d.d_date_sk
WHERE
    d.d_year IS NOT NULL
GROUP BY
    d.d_year

UNION ALL

-- 网站销售 (Web Sales)
SELECT
    d.d_year AS report_year,
    'Web' AS channel,
    SUM(COALESCE(ws.ws_net_paid_inc_ship_tax, 0)) AS total_sales_amount_inc_tax_ship,
    SUM(COALESCE(ws.ws_net_profit, 0)) AS total_net_profit,
    COUNT(DISTINCT ws.ws_order_number) AS total_orders,
    CASE
        WHEN COUNT(DISTINCT ws.ws_order_number) > 0 THEN SUM(COALESCE(ws.ws_net_paid_inc_ship_tax, 0)) / COUNT(DISTINCT ws.ws_order_number)
        ELSE 0
    END AS average_transaction_value
FROM
    web_sales ws
JOIN
    date_dim d ON ws.ws_sold_date_sk = d.d_date_sk
WHERE
    d.d_year IS NOT NULL
GROUP BY
    d.d_year;

--指标2：按客户人口统计特征（教育程度、性别）分析各州（State）的平均消费额
-- 目的：了解不同客户群体在各州的消费能力，用于市场细分和区域营销策略。
-- 报表字段：州, 教育程度, 性别, 客户数, 总消费额(净支付), 人均消费额。
-- 创建按客户画像和州的消费分析报表
CREATE TABLE IF NOT EXISTS customer_segment_state_spending_report (
    customer_state CHAR(2),
    education_status CHAR(20),
    gender CHAR(1),
    number_of_customers BIGINT,
    total_spending_net_paid NUMERIC(18, 2),
    avg_spending_per_customer NUMERIC(18, 2)
);

TRUNCATE TABLE customer_segment_state_spending_report;

INSERT INTO customer_segment_state_spending_report (customer_state, education_status, gender, number_of_customers, total_spending_net_paid, avg_spending_per_customer)
SELECT
    ca.ca_state AS customer_state,
    cd.cd_education_status AS education_status,
    cd.cd_gender AS gender,
    COUNT(DISTINCT c.c_customer_sk) AS number_of_customers,
    SUM(COALESCE(ss.ss_net_paid, 0)) AS total_spending_net_paid, -- 使用交易中的净支付额
    CASE
        WHEN COUNT(DISTINCT c.c_customer_sk) > 0 THEN SUM(COALESCE(ss.ss_net_paid, 0)) / COUNT(DISTINCT c.c_customer_sk)
        ELSE 0
    END AS avg_spending_per_customer
FROM
    store_sales ss  -- 以实体店销售为例，可以扩展到其他或所有渠道
JOIN
    customer c ON ss.ss_customer_sk = c.c_customer_sk
JOIN
    customer_demographics cd ON ss.ss_cdemo_sk = cd.cd_demo_sk -- 使用销售记录关联的人口统计SK
JOIN
    customer_address ca ON ss.ss_addr_sk = ca.ca_address_sk -- 使用销售记录关联的地址SK
WHERE
    ca.ca_state IS NOT NULL AND TRIM(ca.ca_state) != ''
    AND cd.cd_education_status IS NOT NULL AND TRIM(cd.cd_education_status) != ''
    AND cd.cd_gender IS NOT NULL AND TRIM(cd.cd_gender) != ''
GROUP BY
    ca.ca_state,
    cd.cd_education_status,
    cd.cd_gender
HAVING
    COUNT(DISTINCT c.c_customer_sk) > 0; -- 仅显示有客户的细分

-- 指标3：Top 10 热销商品（按销售额）及其主要促销活动名称
-- 目的：找出最畅销的商品，并识别这些商品销售中贡献最大的促销活动。
-- 报表字段：商品ID, 商品名称, 品牌, 类别, 总销售额(扩展售价), 最主要的促销活动名称, 该促销活动贡献的销售额。
-- 创建Top N热销商品及促销分析报表
CREATE TABLE IF NOT EXISTS top_selling_items_promo_analysis_report (
    item_id CHAR(16),
    item_product_name CHAR(50),
    item_brand CHAR(50),
    item_category CHAR(50),
    total_sales_ext_price NUMERIC(18, 2),
    primary_promo_id CHAR(16),
    primary_promo_name CHAR(50),
    primary_promo_sales_contribution NUMERIC(18,2)
);

TRUNCATE TABLE top_selling_items_promo_analysis_report;

INSERT INTO top_selling_items_promo_analysis_report (item_id, item_product_name, item_brand, item_category, total_sales_ext_price, primary_promo_id, primary_promo_name, primary_promo_sales_contribution)
WITH all_sales AS (
    SELECT ss_item_sk AS item_sk, ss_promo_sk AS promo_sk, COALESCE(ss_ext_sales_price, 0) AS sales_value FROM store_sales
    UNION ALL
    SELECT cs_item_sk AS item_sk, cs_promo_sk AS promo_sk, COALESCE(cs_ext_sales_price, 0) AS sales_value FROM catalog_sales
    UNION ALL
    SELECT ws_item_sk AS item_sk, ws_promo_sk AS promo_sk, COALESCE(ws_ext_sales_price, 0) AS sales_value FROM web_sales
),
item_total_sales AS (
    SELECT
        item_sk,
        SUM(sales_value) AS total_item_sales
    FROM all_sales
    GROUP BY item_sk
),
item_promo_sales AS (
    SELECT
        item_sk,
        promo_sk,
        SUM(sales_value) AS promo_specific_sales
    FROM all_sales
    WHERE promo_sk IS NOT NULL
    GROUP BY item_sk, promo_sk
),
ranked_item_promo_sales AS (
    SELECT
        ips.item_sk,
        ips.promo_sk,
        p.p_promo_id,
        p.p_promo_name,
        ips.promo_specific_sales,
        ROW_NUMBER() OVER (PARTITION BY ips.item_sk ORDER BY ips.promo_specific_sales DESC) as rn
    FROM item_promo_sales ips
    JOIN promotion p ON ips.promo_sk = p.p_promo_sk
),
top_items AS (
    SELECT
        its.item_sk,
        its.total_item_sales,
        RANK() OVER (ORDER BY its.total_item_sales DESC) as sales_rank
    FROM item_total_sales its
)
SELECT
    i.i_item_id AS item_id,
    i.i_product_name AS item_product_name,
    i.i_brand AS item_brand,
    i.i_category AS item_category,
    ti.total_item_sales AS total_sales_ext_price,
    rips.p_promo_id AS primary_promo_id,
    rips.p_promo_name AS primary_promo_name,
    COALESCE(rips.promo_specific_sales, 0) AS primary_promo_sales_contribution
FROM
    top_items ti
JOIN
    item i ON ti.item_sk = i.i_item_sk
LEFT JOIN -- 用LEFT JOIN以防商品无促销或主要促销不明确
    ranked_item_promo_sales rips ON ti.item_sk = rips.item_sk AND rips.rn = 1 -- 取每个商品贡献最大的那个促销
WHERE
    ti.sales_rank <= 10 -- 取Top 10热销商品
ORDER BY
    ti.total_item_sales DESC;

-- 指标4：月度各渠道退货率及主要退货原因分析
-- 目的：监控各渠道的退货情况，识别主要的退货原因，以便改进产品或服务。
-- 报表字段：年月, 渠道, 总销售量, 总退货量, 退货率(按数量), 主要退货原因描述, 该原因退货数量。
-- 创建月度渠道退货分析报表
CREATE TABLE IF NOT EXISTS monthly_channel_returns_analysis_report (
    sales_year_month VARCHAR(7), -- YYYY-MM
    channel VARCHAR(20),
    total_sold_quantity BIGINT,
    total_returned_quantity BIGINT,
    return_rate_by_quantity NUMERIC(7, 4),
    primary_return_reason_desc CHAR(100),
    primary_reason_returned_quantity BIGINT
);

TRUNCATE TABLE monthly_channel_returns_analysis_report;

WITH monthly_sales_base AS (
    SELECT 'Store' as channel, ss_sold_date_sk as sold_date_sk, COALESCE(ss_quantity, 0) as quantity FROM store_sales
    UNION ALL
    SELECT 'Catalog' as channel, cs_sold_date_sk as sold_date_sk, COALESCE(cs_quantity, 0) as quantity FROM catalog_sales
    UNION ALL
    SELECT 'Web' as channel, ws_sold_date_sk as sold_date_sk, COALESCE(ws_quantity, 0) as quantity FROM web_sales
),
monthly_returns_base AS (
    SELECT 'Store' as channel, sr_returned_date_sk as returned_date_sk, sr_reason_sk as reason_sk, COALESCE(sr_return_quantity, 0) as quantity FROM store_returns
    UNION ALL
    SELECT 'Catalog' as channel, cr_returned_date_sk as returned_date_sk, cr_reason_sk as reason_sk, COALESCE(cr_return_quantity, 0) as quantity FROM catalog_returns
    UNION ALL
    SELECT 'Web' as channel, wr_returned_date_sk as returned_date_sk, wr_reason_sk as reason_sk, COALESCE(wr_return_quantity, 0) as quantity FROM web_returns
),
monthly_channel_sales AS (
    SELECT
        TO_CHAR(d.d_date, 'YYYY-MM') AS sales_ym,
        msb.channel,
        SUM(msb.quantity) AS total_sold_quantity
    FROM monthly_sales_base msb
    JOIN date_dim d ON msb.sold_date_sk = d.d_date_sk
    WHERE d.d_date IS NOT NULL
    GROUP BY TO_CHAR(d.d_date, 'YYYY-MM'), msb.channel
),
monthly_channel_returns_with_reason AS (
    SELECT
        TO_CHAR(d.d_date, 'YYYY-MM') AS return_ym,
        mrb.channel,
        mrb.reason_sk,
        r.r_reason_desc,
        SUM(mrb.quantity) AS returned_quantity_for_reason
    FROM monthly_returns_base mrb
    JOIN date_dim d ON mrb.returned_date_sk = d.d_date_sk
    LEFT JOIN reason r ON mrb.reason_sk = r.r_reason_sk
    WHERE d.d_date IS NOT NULL
    GROUP BY TO_CHAR(d.d_date, 'YYYY-MM'), mrb.channel, mrb.reason_sk, r.r_reason_desc
),
ranked_monthly_channel_returns AS (
    SELECT
        return_ym,
        channel,
        reason_sk,
        r_reason_desc,
        returned_quantity_for_reason,
        SUM(returned_quantity_for_reason) OVER (PARTITION BY return_ym, channel) as total_returned_in_month_channel,
        ROW_NUMBER() OVER (PARTITION BY return_ym, channel ORDER BY returned_quantity_for_reason DESC) as rn
    FROM monthly_channel_returns_with_reason
)
INSERT INTO monthly_channel_returns_analysis_report (sales_year_month, channel, total_sold_quantity, total_returned_quantity, return_rate_by_quantity, primary_return_reason_desc, primary_reason_returned_quantity)
SELECT
    mcs.sales_ym AS sales_year_month,
    mcs.channel,
    COALESCE(mcs.total_sold_quantity, 0) AS total_sold_quantity,
    COALESCE(rmcr.total_returned_in_month_channel, 0) AS total_returned_quantity,
    CASE
        WHEN COALESCE(mcs.total_sold_quantity, 0) > 0 THEN ROUND(CAST(COALESCE(rmcr.total_returned_in_month_channel, 0) AS NUMERIC) * 100.0 / mcs.total_sold_quantity, 4)
        ELSE 0
    END AS return_rate_by_quantity,
    rmcr.r_reason_desc AS primary_return_reason_desc,
    COALESCE(rmcr.returned_quantity_for_reason, 0) AS primary_reason_returned_quantity
FROM
    monthly_channel_sales mcs
LEFT JOIN -- Join sales with returns data for the same month and channel
    (SELECT * FROM ranked_monthly_channel_returns WHERE rn = 1) rmcr ON mcs.sales_ym = rmcr.return_ym AND mcs.channel = rmcr.channel
ORDER BY
    mcs.sales_ym, mcs.channel;

-- 指标5：不同收入等级家庭的商品大类偏好度 (按销售额占比)
-- 目的：分析不同收入水平的家庭对哪些商品类别更感兴趣，用于产品推荐和市场定位。
-- 报表字段：收入等级描述, 商品大类, 该大类销售额, 该大类销售额占此收入等级客户总销售额的百分比。
-- 创建不同收入等级家庭的商品类别偏好报表
CREATE TABLE IF NOT EXISTS income_level_category_preference_report (
    income_band_description VARCHAR(50), -- e.g., "0-10000" or "10001-20000"
    item_category CHAR(50),
    category_sales_amount NUMERIC(18, 2),
    category_sales_as_percentage_of_income_band_total NUMERIC(5, 2)
);

TRUNCATE TABLE income_level_category_preference_report;

INSERT INTO income_level_category_preference_report (income_band_description, item_category, category_sales_amount, category_sales_as_percentage_of_income_band_total)
WITH customer_sales_by_income_category AS (
    SELECT
        ib.ib_income_band_sk, -- Keep SK for joining
        TRIM(TRAILING FROM CAST(ib.ib_lower_bound AS VARCHAR)) || '-' || TRIM(TRAILING FROM CAST(ib.ib_upper_bound AS VARCHAR)) AS income_band_desc,
        i.i_category AS item_category,
        SUM(COALESCE(ss.ss_ext_sales_price, 0)) AS category_sales -- 使用扩展售价作为衡量标准
    FROM
        store_sales ss -- 以实体店销售为例
    JOIN
        household_demographics hd ON ss.ss_hdemo_sk = hd.hd_demo_sk -- 使用销售记录中的家庭人口统计SK
    JOIN
        income_band ib ON hd.hd_income_band_sk = ib.ib_income_band_sk
    JOIN
        item i ON ss.ss_item_sk = i.i_item_sk
    WHERE
        ib.ib_lower_bound IS NOT NULL AND ib.ib_upper_bound IS NOT NULL
        AND i.i_category IS NOT NULL AND TRIM(i.i_category) != ''
    GROUP BY
        ib.ib_income_band_sk,
        TRIM(TRAILING FROM CAST(ib.ib_lower_bound AS VARCHAR)) || '-' || TRIM(TRAILING FROM CAST(ib.ib_upper_bound AS VARCHAR)),
        i.i_category
),
income_band_total_sales AS (
    SELECT
        csbic.ib_income_band_sk,
        SUM(csbic.category_sales) AS total_sales_for_income_band
    FROM
        customer_sales_by_income_category csbic
    GROUP BY
        csbic.ib_income_band_sk
)
SELECT
    csbic.income_band_desc AS income_band_description,
    csbic.item_category,
    csbic.category_sales AS category_sales_amount,
    CASE
        WHEN ibts.total_sales_for_income_band > 0 THEN ROUND((csbic.category_sales * 100.0 / ibts.total_sales_for_income_band), 2)
        ELSE 0
    END AS category_sales_as_percentage_of_income_band_total
FROM
    customer_sales_by_income_category csbic
JOIN
    income_band_total_sales ibts ON csbic.ib_income_band_sk = ibts.ib_income_band_sk
ORDER BY
    csbic.income_band_desc, category_sales_as_percentage_of_income_band_total DESC;

-- 复杂报表1：客户RFM精细分层及各层级客户行为模式分析
-- 目的：通过RFM（Recency, Frequency, Monetary）模型对客户进行精细化分层，并分析不同层级客户的平均订单价值、主要购买商品品类偏好以及平均复购周期。这有助于企业识别高价值客户、潜力客户、需激活客户和流失风险客户，并制定差异化的营销和服务策略。
-- 报表字段：客户SK, R得分, F得分, M得分, RFM综合得分, RFM客户层级标签, 最近一次购买日期, 购买频率, 总购买金额, 平均订单金额, 平均复购间隔天数, 最常购买商品大类。
-- 创建客户RFM精细分层及行为分析报表
CREATE TABLE IF NOT EXISTS customer_rfm_segmentation_report (
    c_customer_sk INTEGER,
    last_purchase_date DATE,
    recency_score INTEGER,         -- R得分 (1-5, 5为最近)
    frequency_actual BIGINT,
    frequency_score INTEGER,       -- F得分 (1-5, 5为频率最高)
    monetary_value_actual NUMERIC(15,2),
    monetary_score INTEGER,        -- M得分 (1-5, 5为金额最大)
    rfm_cell_id VARCHAR(3),        -- R_F_M, e.g., "555"
    rfm_segment_label VARCHAR(50), -- 如: 高价值客户, 潜力客户等
    avg_order_value NUMERIC(15,2),
    avg_interpurchase_days NUMERIC(10,2), -- 平均复购间隔天数
    preferred_item_category CHAR(50)   -- 最常购买的商品大类
);

TRUNCATE TABLE customer_rfm_segmentation_report;

INSERT INTO customer_rfm_segmentation_report
WITH customer_sales_base AS ( -- 统一所有渠道的销售数据
    SELECT
        ss_customer_sk AS customer_sk,
        ss_sold_date_sk AS sold_date_sk,
        ss_ticket_number AS order_id, -- 用于区分订单
        COALESCE(ss_net_paid_inc_tax, ss_net_paid, 0) AS sales_amount,
        ss_item_sk AS item_sk
    FROM store_sales WHERE ss_customer_sk IS NOT NULL AND ss_sold_date_sk IS NOT NULL
    UNION ALL
    SELECT
        cs_bill_customer_sk AS customer_sk,
        cs_sold_date_sk AS sold_date_sk,
        cs_order_number AS order_id,
        COALESCE(cs_net_paid_inc_ship_tax, cs_net_paid_inc_tax, cs_net_paid, 0) AS sales_amount,
        cs_item_sk AS item_sk
    FROM catalog_sales WHERE cs_bill_customer_sk IS NOT NULL AND cs_sold_date_sk IS NOT NULL
    UNION ALL
    SELECT
        ws_bill_customer_sk AS customer_sk,
        ws_sold_date_sk AS sold_date_sk,
        ws_order_number AS order_id,
        COALESCE(ws_net_paid_inc_ship_tax, ws_net_paid_inc_tax, ws_net_paid, 0) AS sales_amount,
        ws_item_sk AS item_sk
    FROM web_sales WHERE ws_bill_customer_sk IS NOT NULL AND ws_sold_date_sk IS NOT NULL
),
customer_order_dates AS ( -- 获取每个客户的每次订单日期和金额 (去重订单)
    SELECT
        csb.customer_sk,
        d.d_date AS order_date,
        SUM(csb.sales_amount) AS order_amount -- 合并同一订单内多行商品为一笔订单金额
    FROM customer_sales_base csb
    JOIN date_dim d ON csb.sold_date_sk = d.d_date_sk
    GROUP BY csb.customer_sk, d.d_date, csb.order_id -- order_id 确保是不同的订单
),
customer_purchase_summary AS ( -- 计算客户的R, F, M基础值
    SELECT
        customer_sk,
        MAX(order_date) AS last_purchase_date,
        COUNT(DISTINCT order_date) AS frequency, -- 按天计算频率，若一天内多单算一次，若要算订单数则 COUNT(DISTINCT order_id)
        SUM(order_amount) AS monetary_value
    FROM customer_order_dates
    GROUP BY customer_sk
),
-- 假设分析的当前日期为数据中的最大日期
analysis_date_cte AS (SELECT MAX(d_date) AS current_analysis_date FROM date_dim),

customer_rf_values AS ( -- 计算R和F值 (R是天数，F是次数)
    SELECT
        cps.customer_sk,
        (SELECT current_analysis_date FROM analysis_date_cte) - cps.last_purchase_date AS recency_days,
        cps.last_purchase_date,
        cps.frequency,
        cps.monetary_value
    FROM customer_purchase_summary cps
),
customer_rfm_scores AS ( -- 计算R, F, M得分 (使用NTILE进行5等分)
    SELECT
        customer_sk,
        last_purchase_date,
        recency_days,
        NTILE(5) OVER (ORDER BY recency_days ASC) AS recency_score, -- 天数越少，得分越高(所以ASC)
        frequency,
        NTILE(5) OVER (ORDER BY frequency DESC) AS frequency_score, -- 次数越多，得分越高
        monetary_value,
        NTILE(5) OVER (ORDER BY monetary_value DESC) AS monetary_score -- 金额越多，得分越高
    FROM customer_rf_values
),
customer_interpurchase_interval AS ( -- 计算平均复购周期
    SELECT
        customer_sk,
        AVG(purchase_interval_days) AS avg_interval_days
    FROM (
        SELECT
            customer_sk,
            order_date,
            LAG(order_date, 1) OVER (PARTITION BY customer_sk ORDER BY order_date) AS previous_order_date,
            order_date - LAG(order_date, 1) OVER (PARTITION BY customer_sk ORDER BY order_date) AS purchase_interval_days
        FROM customer_order_dates
    ) sub
    WHERE previous_order_date IS NOT NULL AND purchase_interval_days >= 0 -- 至少购买两次
    GROUP BY customer_sk
),
customer_preferred_category AS ( -- 计算客户最常购买的商品大类
    SELECT
        csb.customer_sk,
        i.i_category,
        ROW_NUMBER() OVER (PARTITION BY csb.customer_sk ORDER BY COUNT(csb.item_sk) DESC, SUM(csb.sales_amount) DESC) as rn
    FROM customer_sales_base csb
    JOIN item i ON csb.item_sk = i.i_item_sk
    WHERE i.i_category IS NOT NULL
    GROUP BY csb.customer_sk, i.i_category
),
customer_avg_order_value AS (
    SELECT
        customer_sk,
        AVG(order_amount) as avg_val
    FROM customer_order_dates
    GROUP BY customer_sk
)
SELECT
    rfs.customer_sk AS c_customer_sk,
    rfs.last_purchase_date,
    rfs.recency_score,
    rfs.frequency AS frequency_actual,
    rfs.frequency_score,
    rfs.monetary_value AS monetary_value_actual,
    rfs.monetary_score,
    CAST(rfs.recency_score AS VARCHAR) || CAST(rfs.frequency_score AS VARCHAR) || CAST(rfs.monetary_score AS VARCHAR) AS rfm_cell_id,
    CASE -- 简化的RFM分层逻辑，实际业务会更复杂
        WHEN rfs.recency_score >= 4 AND rfs.frequency_score >= 4 AND rfs.monetary_score >= 4 THEN '高价值客户'
        WHEN rfs.recency_score >= 4 AND rfs.frequency_score <= 2 AND rfs.monetary_score >= 4 THEN '高消费新客'
        WHEN rfs.recency_score <= 2 AND rfs.frequency_score >= 4 AND rfs.monetary_score >= 4 THEN '高价值流失风险客户'
        WHEN rfs.recency_score >= 3 AND rfs.frequency_score >= 3 THEN '忠诚客户'
        WHEN rfs.monetary_score >= 4 THEN '高消费潜力客户'
        WHEN rfs.frequency_score >= 4 THEN '高频率客户'
        WHEN rfs.recency_score <= 2 AND rfs.frequency_score <=2 THEN '沉睡客户'
        ELSE '一般客户'
    END AS rfm_segment_label,
    COALESCE(caov.avg_val, 0) AS avg_order_value,
    COALESCE(cpi.avg_interval_days, NULL) AS avg_interpurchase_days, -- 如果购买次数少于2，则为NULL
    cpc.i_category AS preferred_item_category
FROM
    customer_rfm_scores rfs
LEFT JOIN
    customer_interpurchase_interval cpi ON rfs.customer_sk = cpi.customer_sk
LEFT JOIN
    (SELECT * FROM customer_preferred_category WHERE rn = 1) cpc ON rfs.customer_sk = cpc.customer_sk
LEFT JOIN
    customer_avg_order_value caov ON rfs.customer_sk = caov.customer_sk
ORDER BY rfm_segment_label, monetary_value_actual DESC;

-- 查看报表结果
/* SELECT rfm_segment_label, COUNT(*) as num_customers, AVG(monetary_value_actual) as avg_monetary, AVG(avg_order_value) as overall_avg_order_value, AVG(avg_interpurchase_days) as overall_avg_interpurchase_days
FROM customer_rfm_segmentation_report
GROUP BY rfm_segment_label
ORDER BY avg_monetary DESC; */

--复杂报表2：考虑基线销售的促销活动增量利润与ROI分析
-- 目的：更准确地评估促销活动效果，通过估算“基线销售”（即若无促销活动，该商品/品类同期的预期销售）来计算促销带来的“增量销售额”和“增量净利润”，并结合促销成本计算投资回报率（ROI）。
-- 报表字段：促销ID, 促销名称, 促销商品SK, 促销成本, 促销期间总销售额, 估算基线销售额, 增量销售额, 促销期间总净利润, 估算基线净利润, 增量净利润, ROI。
-- 核心假设： 
--  -- 基线销售：定义为促销活动开始前N天（例如30天）该促销商品的日均销售额/利润额，并乘以促销活动持续天数。
--  -- 促销成本 (p_cost)：直接取自promotion表。
-- 创建促销活动增量效益及ROI分析报表 (修正版)
CREATE TABLE IF NOT EXISTS promotion_incremental_roi_report (
    p_promo_id CHAR(16),
    p_promo_name CHAR(50),
    p_item_sk INTEGER,                     -- 促销针对的商品SK (如果适用)
    i_item_desc VARCHAR(200),
    promo_start_date DATE,
    promo_end_date DATE,
    promo_duration_days INTEGER,
    p_cost NUMERIC(15,2),                 -- 促销成本
    actual_sales_during_promo NUMERIC(15,2), -- 促销期间实际销售额 (ss_ext_sales_price)
    actual_profit_during_promo NUMERIC(15,2),-- 促销期间实际净利润 (ss_net_profit)
    estimated_baseline_sales NUMERIC(15,2),  -- 估算的基线销售额
    estimated_baseline_profit NUMERIC(15,2), -- 估算的基线净利润
    incremental_sales NUMERIC(15,2),
    incremental_profit NUMERIC(15,2),
    roi NUMERIC(10,4)                      -- (增量利润 - 促销成本) / 促销成本
);

TRUNCATE TABLE promotion_incremental_roi_report;

INSERT INTO promotion_incremental_roi_report
WITH promo_details AS (
    SELECT
        p.p_promo_sk,
        p.p_promo_id,
        p.p_promo_name,
        p.p_item_sk, -- 促销可能针对特定商品
        COALESCE(p.p_cost, 0) AS p_cost,
        sd_start.d_date AS promo_start_date,
        sd_end.d_date AS promo_end_date,
        (sd_end.d_date - sd_start.d_date + 1) AS promo_duration_days
    FROM
        promotion p
    JOIN date_dim sd_start ON p.p_start_date_sk = sd_start.d_date_sk
    JOIN date_dim sd_end ON p.p_end_date_sk = sd_end.d_date_sk
    WHERE sd_end.d_date >= sd_start.d_date -- 确保促销结束日期不早于开始日期
),
-- 假设基线期为促销开始前的30天
baseline_period_sales AS (
    SELECT
        pd.p_promo_sk,
        pd.p_item_sk, -- 基线应针对促销商品
        COALESCE(AVG(daily_sales.sales_amount),0) AS avg_daily_baseline_sales,
        COALESCE(AVG(daily_sales.profit_amount),0) AS avg_daily_baseline_profit
    FROM
        promo_details pd
    LEFT JOIN LATERAL ( -- 对于每个促销，计算其商品在促销开始前的日均销售和利润
        SELECT
            d_sold.d_date, -- Keep for GROUP BY to get daily average
            COALESCE(SUM(ss.ss_ext_sales_price),0) AS sales_amount,
            COALESCE(SUM(ss.ss_net_profit),0) AS profit_amount
        FROM store_sales ss -- 以实体店为例, 实际应UNION ALL各渠道
        JOIN date_dim d_sold ON ss.ss_sold_date_sk = d_sold.d_date_sk --  <-- 修正点：这里之前是 d.d_date_sk
        WHERE
            ss.ss_item_sk = pd.p_item_sk -- 仅当促销针对特定商品时，此基线计算才精确
            AND d_sold.d_date >= (pd.promo_start_date - INTERVAL '30 day') -- 促销开始前30天
            AND d_sold.d_date < pd.promo_start_date
        GROUP BY d_sold.d_date -- 计算每日的，然后取平均
        -- 如果p_item_sk为NULL(非特定商品促销)，此基线逻辑需要调整，例如按品类或整个店铺
    ) daily_sales ON pd.p_item_sk IS NOT NULL -- 仅当促销是商品特定时才计算此基线
    GROUP BY pd.p_promo_sk, pd.p_item_sk
),
sales_during_promo AS (
    SELECT
        ss.ss_promo_sk,
        ss.ss_item_sk, -- 确保按商品汇总，如果促销是商品特定的
        SUM(COALESCE(ss.ss_ext_sales_price, 0)) AS total_promo_period_sales,
        SUM(COALESCE(ss.ss_net_profit, 0)) AS total_promo_period_profit
    FROM store_sales ss -- 以实体店为例, 实际应UNION ALL各渠道
    JOIN date_dim d_sold ON ss.ss_sold_date_sk = d_sold.d_date_sk
    JOIN promo_details pd ON ss.ss_promo_sk = pd.p_promo_sk -- 确保销售发生在促销期间内
                         AND d_sold.d_date >= pd.promo_start_date
                         AND d_sold.d_date <= pd.promo_end_date
    WHERE (pd.p_item_sk IS NULL OR ss.ss_item_sk = pd.p_item_sk) -- 促销商品匹配
    GROUP BY ss.ss_promo_sk, ss.ss_item_sk
)
SELECT
    pd.p_promo_id,
    pd.p_promo_name,
    pd.p_item_sk,
    i.i_item_desc,
    pd.promo_start_date,
    pd.promo_end_date,
    pd.promo_duration_days,
    pd.p_cost,
    COALESCE(sp.total_promo_period_sales, 0) AS actual_sales_during_promo,
    COALESCE(sp.total_promo_period_profit, 0) AS actual_profit_during_promo,
    COALESCE(bps.avg_daily_baseline_sales * pd.promo_duration_days, 0) AS estimated_baseline_sales,
    COALESCE(bps.avg_daily_baseline_profit * pd.promo_duration_days, 0) AS estimated_baseline_profit,
    COALESCE(sp.total_promo_period_sales, 0) - COALESCE(bps.avg_daily_baseline_sales * pd.promo_duration_days, 0) AS incremental_sales,
    COALESCE(sp.total_promo_period_profit, 0) - COALESCE(bps.avg_daily_baseline_profit * pd.promo_duration_days, 0) AS incremental_profit,
    CASE
        WHEN pd.p_cost > 0 THEN
            ( (COALESCE(sp.total_promo_period_profit, 0) - COALESCE(bps.avg_daily_baseline_profit * pd.promo_duration_days, 0)) - pd.p_cost) / pd.p_cost
        ELSE NULL -- 成本为0或负数时ROI无意义或需特殊处理
    END AS roi
FROM
    promo_details pd
LEFT JOIN
    baseline_period_sales bps ON pd.p_promo_sk = bps.p_promo_sk AND (pd.p_item_sk IS NULL AND bps.p_item_sk IS NULL OR pd.p_item_sk = bps.p_item_sk)
LEFT JOIN
    sales_during_promo sp ON pd.p_promo_sk = sp.ss_promo_sk AND (pd.p_item_sk IS NULL AND sp.ss_item_sk IS NULL OR pd.p_item_sk = sp.ss_item_sk)
LEFT JOIN
    item i ON pd.p_item_sk = i.i_item_sk -- 获取商品描述
ORDER BY roi DESC, incremental_profit DESC;

-- 查看报表结果概要
/* SELECT p_promo_name, COUNT(DISTINCT p_item_sk) AS num_evaluated_items_in_promo, AVG(roi) as avg_roi, SUM(incremental_profit) as total_promo_incremental_profit
FROM promotion_incremental_roi_report
WHERE p_item_sk IS NOT NULL -- 只看针对特定商品的促销评估汇总
GROUP BY p_promo_name
ORDER BY total_promo_incremental_profit DESC; */

-- 复杂报表3：商品关联规则挖掘（简易购物篮分析 - 计算商品对的支持度、置信度、提升度）
-- 目的：找出经常被一同购买的商品组合 (A 和 B)，计算它们之间的关联强度 (A购买后有多大概率购买B，以及这种关联是否显著)。为交叉销售、捆绑促销、货架摆放优化提供依据。
-- 报表字段：商品A的SK/ID/名称, 商品B的SK/ID/名称, 支持度(A&B), 置信度(A->B), 提升度(A->B)。
-- 核心逻辑： 
--  1. 确定“购物篮”/“订单”。
--  2. 在每个购物篮内生成所有可能的商品对。
--  3. 统计：总交易次数，含商品A的交易次数，含商品B的交易次数，同时含A和B的交易次数。
--  4. 计算指标。
-- 创建商品关联规则分析报表 (计算商品对的支持度、置信度、提升度) (修正版)
CREATE TABLE IF NOT EXISTS item_association_rules_report (
    item_A_sk INTEGER,
    item_A_name CHAR(50),
    item_B_sk INTEGER,
    item_B_name CHAR(50),
    support_A_and_B NUMERIC(10,6), -- P(A and B) = count(A&B) / total_transactions
    confidence_A_to_B NUMERIC(10,6),-- P(B|A) = count(A&B) / count(A)
    lift_A_to_B NUMERIC(10,4)      -- confidence(A->B) / support(B)
);

TRUNCATE TABLE item_association_rules_report;

INSERT INTO item_association_rules_report
WITH transaction_items AS ( -- 1. 定义交易和其中的商品 (所有渠道汇总)
    SELECT
        CAST(ss_ticket_number AS VARCHAR) AS transaction_id, -- <-- 修正点: 将integer转为VARCHAR
        ss_item_sk AS item_sk
    FROM store_sales
    WHERE ss_item_sk IS NOT NULL AND ss_ticket_number IS NOT NULL
    UNION ALL
    SELECT
        CAST(cs_order_number AS VARCHAR) AS transaction_id,
        cs_item_sk AS item_sk
    FROM catalog_sales
    WHERE cs_item_sk IS NOT NULL AND cs_order_number IS NOT NULL
    UNION ALL
    SELECT
        CAST(ws_order_number AS VARCHAR) AS transaction_id,
        ws_item_sk AS item_sk
    FROM web_sales
    WHERE ws_item_sk IS NOT NULL AND ws_order_number IS NOT NULL
),
distinct_transaction_items AS ( -- 确保每个商品在一个交易中只算一次 (如果一个订单里同个商品有多行)
    SELECT DISTINCT transaction_id, item_sk FROM transaction_items
),
item_pairs_in_transaction AS ( -- 2. 生成同一交易内的商品对 (itemA, itemB) 且 itemA < itemB 避免重复和自我配对
    SELECT
        t1.transaction_id,
        t1.item_sk AS item_A_sk,
        t2.item_sk AS item_B_sk
    FROM distinct_transaction_items t1
    JOIN distinct_transaction_items t2 ON t1.transaction_id = t2.transaction_id AND t1.item_sk < t2.item_sk
),
transaction_counts AS ( -- 3.1. 总交易次数
    SELECT COUNT(DISTINCT transaction_id) AS total_transactions FROM distinct_transaction_items
),
item_counts AS ( -- 3.2. 每个商品出现的交易次数
    SELECT item_sk, COUNT(DISTINCT transaction_id) AS item_transaction_count
    FROM distinct_transaction_items
    GROUP BY item_sk
),
item_pair_counts AS ( -- 3.3. 商品对共同出现的交易次数
    SELECT
        item_A_sk,
        item_B_sk,
        COUNT(DISTINCT transaction_id) AS pair_transaction_count
    FROM item_pairs_in_transaction
    GROUP BY item_A_sk, item_B_sk
)
SELECT
    ipc.item_A_sk,
    ia.i_product_name AS item_A_name,
    ipc.item_B_sk,
    ib.i_product_name AS item_B_name,
    CAST(ipc.pair_transaction_count AS NUMERIC) / tc.total_transactions AS support_A_and_B,
    CASE
        WHEN ic_A.item_transaction_count > 0 THEN CAST(ipc.pair_transaction_count AS NUMERIC) / ic_A.item_transaction_count
        ELSE 0
    END AS confidence_A_to_B,
    CASE
        WHEN ic_A.item_transaction_count > 0 AND ic_B.item_transaction_count > 0 AND tc.total_transactions > 0 AND
             (CAST(ipc.pair_transaction_count AS NUMERIC) / tc.total_transactions) > 0 AND -- Support(B) > 0
             (CAST(ic_B.item_transaction_count AS NUMERIC) / tc.total_transactions) > 0
        THEN (CAST(ipc.pair_transaction_count AS NUMERIC) / ic_A.item_transaction_count) / (CAST(ic_B.item_transaction_count AS NUMERIC) / tc.total_transactions)
        ELSE 0 -- Or NULL, if lift cannot be meaningfully computed
    END AS lift_A_to_B
FROM
    item_pair_counts ipc
JOIN
    item_counts ic_A ON ipc.item_A_sk = ic_A.item_sk
JOIN
    item_counts ic_B ON ipc.item_B_sk = ic_B.item_sk
CROSS JOIN -- 只有一个总交易数，可以直接交叉连接
    transaction_counts tc
JOIN
    item ia ON ipc.item_A_sk = ia.i_item_sk -- 获取商品A名称
JOIN
    item ib ON ipc.item_B_sk = ib.i_item_sk -- 获取商品B名称
WHERE
    ic_A.item_transaction_count > 0 AND ic_B.item_transaction_count > 0 -- 避免除以0
    AND (CAST(ipc.pair_transaction_count AS NUMERIC) / tc.total_transactions) > 0.0001 -- 最小支持度筛选，根据数据量调整
    AND (CASE WHEN ic_A.item_transaction_count > 0 THEN CAST(ipc.pair_transaction_count AS NUMERIC) / ic_A.item_transaction_count ELSE 0 END) > 0.001 -- 最小置信度筛选
ORDER BY
    lift_A_to_B DESC, confidence_A_to_B DESC
LIMIT 500; -- 限制输出结果数量，实际中可能非常多

-- 查看部分结果，例如高提升度的商品对
-- SELECT * FROM item_association_rules_report ORDER BY lift_A_to_B DESC, confidence_A_to_B DESC LIMIT 20;