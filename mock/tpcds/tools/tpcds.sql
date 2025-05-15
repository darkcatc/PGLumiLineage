-- 
-- Legal Notice 
-- 
-- This document and associated source code (the "Work") is a part of a 
-- benchmark specification maintained by the TPC. 
-- 
-- The TPC reserves all right, title, and interest to the Work as provided 
-- under U.S. and international laws, including without limitation all patent 
-- and trademark rights therein. 
-- 
-- No Warranty 
-- 
-- 1.1 TO THE MAXIMUM EXTENT PERMITTED BY APPLICABLE LAW, THE INFORMATION 
--     CONTAINED HEREIN IS PROVIDED "AS IS" AND WITH ALL FAULTS, AND THE 
--     AUTHORS AND DEVELOPERS OF THE WORK HEREBY DISCLAIM ALL OTHER 
--     WARRANTIES AND CONDITIONS, EITHER EXPRESS, IMPLIED OR STATUTORY, 
--     INCLUDING, BUT NOT LIMITED TO, ANY (IF ANY) IMPLIED WARRANTIES, 
--     DUTIES OR CONDITIONS OF MERCHANTABILITY, OF FITNESS FOR A PARTICULAR 
--     PURPOSE, OF ACCURACY OR COMPLETENESS OF RESPONSES, OF RESULTS, OF 
--     WORKMANLIKE EFFORT, OF LACK OF VIRUSES, AND OF LACK OF NEGLIGENCE. 
--     ALSO, THERE IS NO WARRANTY OR CONDITION OF TITLE, QUIET ENJOYMENT, 
--     QUIET POSSESSION, CORRESPONDENCE TO DESCRIPTION OR NON-INFRINGEMENT 
--     WITH REGARD TO THE WORK. 
-- 1.2 IN NO EVENT WILL ANY AUTHOR OR DEVELOPER OF THE WORK BE LIABLE TO 
--     ANY OTHER PARTY FOR ANY DAMAGES, INCLUDING BUT NOT LIMITED TO THE 
--     COST OF PROCURING SUBSTITUTE GOODS OR SERVICES, LOST PROFITS, LOSS 
--     OF USE, LOSS OF DATA, OR ANY INCIDENTAL, CONSEQUENTIAL, DIRECT, 
--     INDIRECT, OR SPECIAL DAMAGES WHETHER UNDER CONTRACT, TORT, WARRANTY,
--     OR OTHERWISE, ARISING IN ANY WAY OUT OF THIS OR ANY OTHER AGREEMENT 
--     RELATING TO THE WORK, WHETHER OR NOT SUCH AUTHOR OR DEVELOPER HAD 
--     ADVANCE NOTICE OF THE POSSIBILITY OF SUCH DAMAGES. 
-- 
-- Contributors:
-- Gradient Systems
--
-- 1. 确保日志收集器是开启的 (通常默认是 on，但显式设置更保险)
ALTER SYSTEM SET logging_collector = on;

-- 2. 设置日志输出格式为 CSV，同时也输出到 stderr (方便实时查看)
ALTER SYSTEM SET log_destination = 'csvlog, stderr';

-- 3. 指定 CSV 日志中要包含的列 (非常重要，确保包含所有需要的信息)
--    至少应包含：log_time, user_name, database_name, process_id, remote_host_and_port,
--    session_id, session_line_num, command_tag, session_start_time, virtual_transaction_id,
--    transaction_id, error_severity, sql_state_code, message, detail, hint,
--    internal_query, internal_query_pos, context, query, query_pos, location,
--    application_name, backend_type, leader_pid, query_id (PG14+)
--    具体可以参考 `log_line_prefix` 和 CSV 日志的默认列。
--    对于 CSV 日志，很多信息是自动包含的，但 `log_line_prefix` 仍然对 stderr 输出格式有影响。
--    可以考虑设置 `log_line_prefix` 来丰富 stderr 输出，例如：
--    ALTER SYSTEM SET log_line_prefix = '%m [%p] %q%u@%d/%a R:%h S:%s C:%c L:%l '; -- 这是一个比较详细的例子

-- 4. 记录 DDL 和修改性 DML 语句 (INSERT, UPDATE, DELETE)
ALTER SYSTEM SET log_statement = 'mod';

-- 5. 记录执行时间超过 N 毫秒的语句 (例如，5秒 = 5000毫秒)
--    这将捕获慢的 SELECT 语句以及其他类型的慢查询。
ALTER SYSTEM SET log_min_duration_statement = 5000; -- (5秒, 可以根据你的需求调整)

-- 6. (可选, 但推荐) 设置日志文件名格式和轮转，不过这些通常在 postgresql.conf 中设置更全面
ALTER SYSTEM SET log_filename = 'postgresql-%Y-%m-%d_%H%M%S.log'; -- (如果是 stderr 且 logging_collector=on)
ALTER SYSTEM SET log_truncate_on_rotation = on;
ALTER SYSTEM SET log_rotation_age = '1d';
ALTER SYSTEM SET log_rotation_size = '500MB'; -- (这些更适合在 postgresql.conf 中调整)

-- 7. 应用更改
SELECT pg_reload_conf();

create table dbgen_version
(
    dv_version                varchar(16)                   ,
    dv_create_date            date                          ,
    dv_create_time            time                          ,
    dv_cmdline_args           varchar(200)                  
);

create table customer_address
(
    ca_address_sk             integer               not null,
    ca_address_id             char(16)              not null,
    ca_street_number          char(10)                      ,
    ca_street_name            varchar(60)                   ,
    ca_street_type            char(15)                      ,
    ca_suite_number           char(10)                      ,
    ca_city                   varchar(60)                   ,
    ca_county                 varchar(30)                   ,
    ca_state                  char(2)                       ,
    ca_zip                    char(10)                      ,
    ca_country                varchar(20)                   ,
    ca_gmt_offset             decimal(5,2)                  ,
    ca_location_type          char(20)                      ,
    primary key (ca_address_sk)
);

create table customer_demographics
(
    cd_demo_sk                integer               not null,
    cd_gender                 char(1)                       ,
    cd_marital_status         char(1)                       ,
    cd_education_status       char(20)                      ,
    cd_purchase_estimate      integer                       ,
    cd_credit_rating          char(10)                      ,
    cd_dep_count              integer                       ,
    cd_dep_employed_count     integer                       ,
    cd_dep_college_count      integer                       ,
    primary key (cd_demo_sk)
);

create table date_dim
(
    d_date_sk                 integer               not null,
    d_date_id                 char(16)              not null,
    d_date                    date                  not null,
    d_month_seq               integer                       ,
    d_week_seq                integer                       ,
    d_quarter_seq             integer                       ,
    d_year                    integer                       ,
    d_dow                     integer                       ,
    d_moy                     integer                       ,
    d_dom                     integer                       ,
    d_qoy                     integer                       ,
    d_fy_year                 integer                       ,
    d_fy_quarter_seq          integer                       ,
    d_fy_week_seq             integer                       ,
    d_day_name                char(9)                       ,
    d_quarter_name            char(6)                       ,
    d_holiday                 char(1)                       ,
    d_weekend                 char(1)                       ,
    d_following_holiday       char(1)                       ,
    d_first_dom               integer                       ,
    d_last_dom                integer                       ,
    d_same_day_ly             integer                       ,
    d_same_day_lq             integer                       ,
    d_current_day             char(1)                       ,
    d_current_week            char(1)                       ,
    d_current_month           char(1)                       ,
    d_current_quarter         char(1)                       ,
    d_current_year            char(1)                       ,
    primary key (d_date_sk)
);

create table warehouse
(
    w_warehouse_sk            integer               not null,
    w_warehouse_id            char(16)              not null,
    w_warehouse_name          varchar(20)                   ,
    w_warehouse_sq_ft         integer                       ,
    w_street_number           char(10)                      ,
    w_street_name             varchar(60)                   ,
    w_street_type             char(15)                      ,
    w_suite_number            char(10)                      ,
    w_city                    varchar(60)                   ,
    w_county                  varchar(30)                   ,
    w_state                   char(2)                       ,
    w_zip                     char(10)                      ,
    w_country                 varchar(20)                   ,
    w_gmt_offset              decimal(5,2)                  ,
    primary key (w_warehouse_sk)
);

create table ship_mode
(
    sm_ship_mode_sk           integer               not null,
    sm_ship_mode_id           char(16)              not null,
    sm_type                   char(30)                      ,
    sm_code                   char(10)                      ,
    sm_carrier                char(20)                      ,
    sm_contract               char(20)                      ,
    primary key (sm_ship_mode_sk)
);

create table time_dim
(
    t_time_sk                 integer               not null,
    t_time_id                 char(16)              not null,
    t_time                    integer               not null,
    t_hour                    integer                       ,
    t_minute                  integer                       ,
    t_second                  integer                       ,
    t_am_pm                   char(2)                       ,
    t_shift                   char(20)                      ,
    t_sub_shift               char(20)                      ,
    t_meal_time               char(20)                      ,
    primary key (t_time_sk)
);

create table reason
(
    r_reason_sk               integer               not null,
    r_reason_id               char(16)              not null,
    r_reason_desc             char(100)                     ,
    primary key (r_reason_sk)
);

create table income_band
(
    ib_income_band_sk         integer               not null,
    ib_lower_bound            integer                       ,
    ib_upper_bound            integer                       ,
    primary key (ib_income_band_sk)
);

create table item
(
    i_item_sk                 integer               not null,
    i_item_id                 char(16)              not null,
    i_rec_start_date          date                          ,
    i_rec_end_date            date                          ,
    i_item_desc               varchar(200)                  ,
    i_current_price           decimal(7,2)                  ,
    i_wholesale_cost          decimal(7,2)                  ,
    i_brand_id                integer                       ,
    i_brand                   char(50)                      ,
    i_class_id                integer                       ,
    i_class                   char(50)                      ,
    i_category_id             integer                       ,
    i_category                char(50)                      ,
    i_manufact_id             integer                       ,
    i_manufact                char(50)                      ,
    i_size                    char(20)                      ,
    i_formulation             char(20)                      ,
    i_color                   char(20)                      ,
    i_units                   char(10)                      ,
    i_container               char(10)                      ,
    i_manager_id              integer                       ,
    i_product_name            char(50)                      ,
    primary key (i_item_sk)
);

create table store
(
    s_store_sk                integer               not null,
    s_store_id                char(16)              not null,
    s_rec_start_date          date                          ,
    s_rec_end_date            date                          ,
    s_closed_date_sk          integer                       ,
    s_store_name              varchar(50)                   ,
    s_number_employees        integer                       ,
    s_floor_space             integer                       ,
    s_hours                   char(20)                      ,
    s_manager                 varchar(40)                   ,
    s_market_id               integer                       ,
    s_geography_class         varchar(100)                  ,
    s_market_desc             varchar(100)                  ,
    s_market_manager          varchar(40)                   ,
    s_division_id             integer                       ,
    s_division_name           varchar(50)                   ,
    s_company_id              integer                       ,
    s_company_name            varchar(50)                   ,
    s_street_number           varchar(10)                   ,
    s_street_name             varchar(60)                   ,
    s_street_type             char(15)                      ,
    s_suite_number            char(10)                      ,
    s_city                    varchar(60)                   ,
    s_county                  varchar(30)                   ,
    s_state                   char(2)                       ,
    s_zip                     char(10)                      ,
    s_country                 varchar(20)                   ,
    s_gmt_offset              decimal(5,2)                  ,
    s_tax_precentage          decimal(5,2)                  ,
    primary key (s_store_sk)
);

create table call_center
(
    cc_call_center_sk         integer               not null,
    cc_call_center_id         char(16)              not null,
    cc_rec_start_date         date                          ,
    cc_rec_end_date           date                          ,
    cc_closed_date_sk         integer                       ,
    cc_open_date_sk           integer                       ,
    cc_name                   varchar(50)                   ,
    cc_class                  varchar(50)                   ,
    cc_employees              integer                       ,
    cc_sq_ft                  integer                       ,
    cc_hours                  char(20)                      ,
    cc_manager                varchar(40)                   ,
    cc_mkt_id                 integer                       ,
    cc_mkt_class              char(50)                      ,
    cc_mkt_desc               varchar(100)                  ,
    cc_market_manager         varchar(40)                   ,
    cc_division               integer                       ,
    cc_division_name          varchar(50)                   ,
    cc_company                integer                       ,
    cc_company_name           char(50)                      ,
    cc_street_number          char(10)                      ,
    cc_street_name            varchar(60)                   ,
    cc_street_type            char(15)                      ,
    cc_suite_number           char(10)                      ,
    cc_city                   varchar(60)                   ,
    cc_county                 varchar(30)                   ,
    cc_state                  char(2)                       ,
    cc_zip                    char(10)                      ,
    cc_country                varchar(20)                   ,
    cc_gmt_offset             decimal(5,2)                  ,
    cc_tax_percentage         decimal(5,2)                  ,
    primary key (cc_call_center_sk)
);

create table customer
(
    c_customer_sk             integer               not null,
    c_customer_id             char(16)              not null,
    c_current_cdemo_sk        integer                       ,
    c_current_hdemo_sk        integer                       ,
    c_current_addr_sk         integer                       ,
    c_first_shipto_date_sk    integer                       ,
    c_first_sales_date_sk     integer                       ,
    c_salutation              char(10)                      ,
    c_first_name              char(20)                      ,
    c_last_name               char(30)                      ,
    c_preferred_cust_flag     char(1)                       ,
    c_birth_day               integer                       ,
    c_birth_month             integer                       ,
    c_birth_year              integer                       ,
    c_birth_country           varchar(20)                   ,
    c_login                   char(13)                      ,
    c_email_address           char(50)                      ,
    c_last_review_date        char(10)                      ,
    primary key (c_customer_sk)
);

create table web_site
(
    web_site_sk               integer               not null,
    web_site_id               char(16)              not null,
    web_rec_start_date        date                          ,
    web_rec_end_date          date                          ,
    web_name                  varchar(50)                   ,
    web_open_date_sk          integer                       ,
    web_close_date_sk         integer                       ,
    web_class                 varchar(50)                   ,
    web_manager               varchar(40)                   ,
    web_mkt_id                integer                       ,
    web_mkt_class             varchar(50)                   ,
    web_mkt_desc              varchar(100)                  ,
    web_market_manager        varchar(40)                   ,
    web_company_id            integer                       ,
    web_company_name          char(50)                      ,
    web_street_number         char(10)                      ,
    web_street_name           varchar(60)                   ,
    web_street_type           char(15)                      ,
    web_suite_number          char(10)                      ,
    web_city                  varchar(60)                   ,
    web_county                varchar(30)                   ,
    web_state                 char(2)                       ,
    web_zip                   char(10)                      ,
    web_country               varchar(20)                   ,
    web_gmt_offset            decimal(5,2)                  ,
    web_tax_percentage        decimal(5,2)                  ,
    primary key (web_site_sk)
);

create table store_returns
(
    sr_returned_date_sk       integer                       ,
    sr_return_time_sk         integer                       ,
    sr_item_sk                integer               not null,
    sr_customer_sk            integer                       ,
    sr_cdemo_sk               integer                       ,
    sr_hdemo_sk               integer                       ,
    sr_addr_sk                integer                       ,
    sr_store_sk               integer                       ,
    sr_reason_sk              integer                       ,
    sr_ticket_number          integer               not null,
    sr_return_quantity        integer                       ,
    sr_return_amt             decimal(7,2)                  ,
    sr_return_tax             decimal(7,2)                  ,
    sr_return_amt_inc_tax     decimal(7,2)                  ,
    sr_fee                    decimal(7,2)                  ,
    sr_return_ship_cost       decimal(7,2)                  ,
    sr_refunded_cash          decimal(7,2)                  ,
    sr_reversed_charge        decimal(7,2)                  ,
    sr_store_credit           decimal(7,2)                  ,
    sr_net_loss               decimal(7,2)                  ,
    primary key (sr_item_sk, sr_ticket_number)
);

create table household_demographics
(
    hd_demo_sk                integer               not null,
    hd_income_band_sk         integer                       ,
    hd_buy_potential          char(15)                      ,
    hd_dep_count              integer                       ,
    hd_vehicle_count          integer                       ,
    primary key (hd_demo_sk)
);

create table web_page
(
    wp_web_page_sk            integer               not null,
    wp_web_page_id            char(16)              not null,
    wp_rec_start_date         date                          ,
    wp_rec_end_date           date                          ,
    wp_creation_date_sk       integer                       ,
    wp_access_date_sk         integer                       ,
    wp_autogen_flag           char(1)                       ,
    wp_customer_sk            integer                       ,
    wp_url                    varchar(100)                  ,
    wp_type                   char(50)                      ,
    wp_char_count             integer                       ,
    wp_link_count             integer                       ,
    wp_image_count            integer                       ,
    wp_max_ad_count           integer                       ,
    primary key (wp_web_page_sk)
);

create table promotion
(
    p_promo_sk                integer               not null,
    p_promo_id                char(16)              not null,
    p_start_date_sk           integer                       ,
    p_end_date_sk             integer                       ,
    p_item_sk                 integer                       ,
    p_cost                    decimal(15,2)                 ,
    p_response_target         integer                       ,
    p_promo_name              char(50)                      ,
    p_channel_dmail           char(1)                       ,
    p_channel_email           char(1)                       ,
    p_channel_catalog         char(1)                       ,
    p_channel_tv              char(1)                       ,
    p_channel_radio           char(1)                       ,
    p_channel_press           char(1)                       ,
    p_channel_event           char(1)                       ,
    p_channel_demo            char(1)                       ,
    p_channel_details         varchar(100)                  ,
    p_purpose                 char(15)                      ,
    p_discount_active         char(1)                       ,
    primary key (p_promo_sk)
);

create table catalog_page
(
    cp_catalog_page_sk        integer               not null,
    cp_catalog_page_id        char(16)              not null,
    cp_start_date_sk          integer                       ,
    cp_end_date_sk            integer                       ,
    cp_department             varchar(50)                   ,
    cp_catalog_number         integer                       ,
    cp_catalog_page_number    integer                       ,
    cp_description            varchar(100)                  ,
    cp_type                   varchar(100)                  ,
    primary key (cp_catalog_page_sk)
);

create table inventory
(
    inv_date_sk               integer               not null,
    inv_item_sk               integer               not null,
    inv_warehouse_sk          integer               not null,
    inv_quantity_on_hand      integer                       ,
    primary key (inv_date_sk, inv_item_sk, inv_warehouse_sk)
);

create table catalog_returns
(
    cr_returned_date_sk       integer                       ,
    cr_returned_time_sk       integer                       ,
    cr_item_sk                integer               not null,
    cr_refunded_customer_sk   integer                       ,
    cr_refunded_cdemo_sk      integer                       ,
    cr_refunded_hdemo_sk      integer                       ,
    cr_refunded_addr_sk       integer                       ,
    cr_returning_customer_sk  integer                       ,
    cr_returning_cdemo_sk     integer                       ,
    cr_returning_hdemo_sk     integer                       ,
    cr_returning_addr_sk      integer                       ,
    cr_call_center_sk         integer                       ,
    cr_catalog_page_sk        integer                       ,
    cr_ship_mode_sk           integer                       ,
    cr_warehouse_sk           integer                       ,
    cr_reason_sk              integer                       ,
    cr_order_number           integer               not null,
    cr_return_quantity        integer                       ,
    cr_return_amount          decimal(7,2)                  ,
    cr_return_tax             decimal(7,2)                  ,
    cr_return_amt_inc_tax     decimal(7,2)                  ,
    cr_fee                    decimal(7,2)                  ,
    cr_return_ship_cost       decimal(7,2)                  ,
    cr_refunded_cash          decimal(7,2)                  ,
    cr_reversed_charge        decimal(7,2)                  ,
    cr_store_credit           decimal(7,2)                  ,
    cr_net_loss               decimal(7,2)                  ,
    primary key (cr_item_sk, cr_order_number)
);

create table web_returns
(
    wr_returned_date_sk       integer                       ,
    wr_returned_time_sk       integer                       ,
    wr_item_sk                integer               not null,
    wr_refunded_customer_sk   integer                       ,
    wr_refunded_cdemo_sk      integer                       ,
    wr_refunded_hdemo_sk      integer                       ,
    wr_refunded_addr_sk       integer                       ,
    wr_returning_customer_sk  integer                       ,
    wr_returning_cdemo_sk     integer                       ,
    wr_returning_hdemo_sk     integer                       ,
    wr_returning_addr_sk      integer                       ,
    wr_web_page_sk            integer                       ,
    wr_reason_sk              integer                       ,
    wr_order_number           integer               not null,
    wr_return_quantity        integer                       ,
    wr_return_amt             decimal(7,2)                  ,
    wr_return_tax             decimal(7,2)                  ,
    wr_return_amt_inc_tax     decimal(7,2)                  ,
    wr_fee                    decimal(7,2)                  ,
    wr_return_ship_cost       decimal(7,2)                  ,
    wr_refunded_cash          decimal(7,2)                  ,
    wr_reversed_charge        decimal(7,2)                  ,
    wr_account_credit         decimal(7,2)                  ,
    wr_net_loss               decimal(7,2)                  ,
    primary key (wr_item_sk, wr_order_number)
);

create table web_sales
(
    ws_sold_date_sk           integer                       ,
    ws_sold_time_sk           integer                       ,
    ws_ship_date_sk           integer                       ,
    ws_item_sk                integer               not null,
    ws_bill_customer_sk       integer                       ,
    ws_bill_cdemo_sk          integer                       ,
    ws_bill_hdemo_sk          integer                       ,
    ws_bill_addr_sk           integer                       ,
    ws_ship_customer_sk       integer                       ,
    ws_ship_cdemo_sk          integer                       ,
    ws_ship_hdemo_sk          integer                       ,
    ws_ship_addr_sk           integer                       ,
    ws_web_page_sk            integer                       ,
    ws_web_site_sk            integer                       ,
    ws_ship_mode_sk           integer                       ,
    ws_warehouse_sk           integer                       ,
    ws_promo_sk               integer                       ,
    ws_order_number           integer               not null,
    ws_quantity               integer                       ,
    ws_wholesale_cost         decimal(7,2)                  ,
    ws_list_price             decimal(7,2)                  ,
    ws_sales_price            decimal(7,2)                  ,
    ws_ext_discount_amt       decimal(7,2)                  ,
    ws_ext_sales_price        decimal(7,2)                  ,
    ws_ext_wholesale_cost     decimal(7,2)                  ,
    ws_ext_list_price         decimal(7,2)                  ,
    ws_ext_tax                decimal(7,2)                  ,
    ws_coupon_amt             decimal(7,2)                  ,
    ws_ext_ship_cost          decimal(7,2)                  ,
    ws_net_paid               decimal(7,2)                  ,
    ws_net_paid_inc_tax       decimal(7,2)                  ,
    ws_net_paid_inc_ship      decimal(7,2)                  ,
    ws_net_paid_inc_ship_tax  decimal(7,2)                  ,
    ws_net_profit             decimal(7,2)                  ,
    primary key (ws_item_sk, ws_order_number)
);

create table catalog_sales
(
    cs_sold_date_sk           integer                       ,
    cs_sold_time_sk           integer                       ,
    cs_ship_date_sk           integer                       ,
    cs_bill_customer_sk       integer                       ,
    cs_bill_cdemo_sk          integer                       ,
    cs_bill_hdemo_sk          integer                       ,
    cs_bill_addr_sk           integer                       ,
    cs_ship_customer_sk       integer                       ,
    cs_ship_cdemo_sk          integer                       ,
    cs_ship_hdemo_sk          integer                       ,
    cs_ship_addr_sk           integer                       ,
    cs_call_center_sk         integer                       ,
    cs_catalog_page_sk        integer                       ,
    cs_ship_mode_sk           integer                       ,
    cs_warehouse_sk           integer                       ,
    cs_item_sk                integer               not null,
    cs_promo_sk               integer                       ,
    cs_order_number           integer               not null,
    cs_quantity               integer                       ,
    cs_wholesale_cost         decimal(7,2)                  ,
    cs_list_price             decimal(7,2)                  ,
    cs_sales_price            decimal(7,2)                  ,
    cs_ext_discount_amt       decimal(7,2)                  ,
    cs_ext_sales_price        decimal(7,2)                  ,
    cs_ext_wholesale_cost     decimal(7,2)                  ,
    cs_ext_list_price         decimal(7,2)                  ,
    cs_ext_tax                decimal(7,2)                  ,
    cs_coupon_amt             decimal(7,2)                  ,
    cs_ext_ship_cost          decimal(7,2)                  ,
    cs_net_paid               decimal(7,2)                  ,
    cs_net_paid_inc_tax       decimal(7,2)                  ,
    cs_net_paid_inc_ship      decimal(7,2)                  ,
    cs_net_paid_inc_ship_tax  decimal(7,2)                  ,
    cs_net_profit             decimal(7,2)                  ,
    primary key (cs_item_sk, cs_order_number)
);

create table store_sales
(
    ss_sold_date_sk           integer                       ,
    ss_sold_time_sk           integer                       ,
    ss_item_sk                integer               not null,
    ss_customer_sk            integer                       ,
    ss_cdemo_sk               integer                       ,
    ss_hdemo_sk               integer                       ,
    ss_addr_sk                integer                       ,
    ss_store_sk               integer                       ,
    ss_promo_sk               integer                       ,
    ss_ticket_number          integer               not null,
    ss_quantity               integer                       ,
    ss_wholesale_cost         decimal(7,2)                  ,
    ss_list_price             decimal(7,2)                  ,
    ss_sales_price            decimal(7,2)                  ,
    ss_ext_discount_amt       decimal(7,2)                  ,
    ss_ext_sales_price        decimal(7,2)                  ,
    ss_ext_wholesale_cost     decimal(7,2)                  ,
    ss_ext_list_price         decimal(7,2)                  ,
    ss_ext_tax                decimal(7,2)                  ,
    ss_coupon_amt             decimal(7,2)                  ,
    ss_net_paid               decimal(7,2)                  ,
    ss_net_paid_inc_tax       decimal(7,2)                  ,
    ss_net_profit             decimal(7,2)                  ,
    primary key (ss_item_sk, ss_ticket_number)
);


-- Comments for table: customer_address
COMMENT ON TABLE customer_address IS 'Stores customer address information.';
COMMENT ON COLUMN customer_address.ca_address_sk IS 'Surrogate key for customer address.';
COMMENT ON COLUMN customer_address.ca_address_id IS 'Business key for customer address.';
COMMENT ON COLUMN customer_address.ca_street_number IS 'Street number of the address.';
COMMENT ON COLUMN customer_address.ca_street_name IS 'Street name of the address.';
COMMENT ON COLUMN customer_address.ca_street_type IS 'Street type (e.g., St, Ave, Rd).';
COMMENT ON COLUMN customer_address.ca_suite_number IS 'Suite or apartment number.';
COMMENT ON COLUMN customer_address.ca_city IS 'City of the address.';
COMMENT ON COLUMN customer_address.ca_county IS 'County of the address.';
COMMENT ON COLUMN customer_address.ca_state IS 'State abbreviation of the address.';
COMMENT ON COLUMN customer_address.ca_zip IS 'ZIP code of the address.';
COMMENT ON COLUMN customer_address.ca_country IS 'Country of the address.';
COMMENT ON COLUMN customer_address.ca_gmt_offset IS 'GMT offset for the address location.';
COMMENT ON COLUMN customer_address.ca_location_type IS 'Type of location (e.g., residential, business).';

-- Comments for table: customer_demographics
COMMENT ON TABLE customer_demographics IS 'Stores customer demographic information.';
COMMENT ON COLUMN customer_demographics.cd_demo_sk IS 'Surrogate key for customer demographics.';
COMMENT ON COLUMN customer_demographics.cd_gender IS 'Gender of the customer (M, F, U).';
COMMENT ON COLUMN customer_demographics.cd_marital_status IS 'Marital status of the customer (M, S, D, W, U).';
COMMENT ON COLUMN customer_demographics.cd_education_status IS 'Highest education level of the customer.';
COMMENT ON COLUMN customer_demographics.cd_purchase_estimate IS 'Estimated purchase amount category for the customer.';
COMMENT ON COLUMN customer_demographics.cd_credit_rating IS 'Credit rating of the customer.';
COMMENT ON COLUMN customer_demographics.cd_dep_count IS 'Number of dependents for the customer.';
COMMENT ON COLUMN customer_demographics.cd_dep_employed_count IS 'Number of employed dependents.';
COMMENT ON COLUMN customer_demographics.cd_dep_college_count IS 'Number of dependents attending college.';

-- Comments for table: date_dim
COMMENT ON TABLE date_dim IS 'Dimension table for dates.';
COMMENT ON COLUMN date_dim.d_date_sk IS 'Surrogate key for date.';
COMMENT ON COLUMN date_dim.d_date_id IS 'Business key for date (e.g., YYYY-MM-DD).';
COMMENT ON COLUMN date_dim.d_date IS 'Actual date value.';
COMMENT ON COLUMN date_dim.d_month_seq IS 'Sequential month number since a base date.';
COMMENT ON COLUMN date_dim.d_week_seq IS 'Sequential week number since a base date.';
COMMENT ON COLUMN date_dim.d_quarter_seq IS 'Sequential quarter number since a base date.';
COMMENT ON COLUMN date_dim.d_year IS 'Year (e.g., 2023).';
COMMENT ON COLUMN date_dim.d_dow IS 'Day of the week (0=Sunday, 1=Monday, ..., 6=Saturday).';
COMMENT ON COLUMN date_dim.d_moy IS 'Month of the year (1-12).';
COMMENT ON COLUMN date_dim.d_dom IS 'Day of the month (1-31).';
COMMENT ON COLUMN date_dim.d_qoy IS 'Quarter of the year (1-4).';
COMMENT ON COLUMN date_dim.d_fy_year IS 'Fiscal year.';
COMMENT ON COLUMN date_dim.d_fy_quarter_seq IS 'Sequential fiscal quarter number.';
COMMENT ON COLUMN date_dim.d_fy_week_seq IS 'Sequential fiscal week number.';
COMMENT ON COLUMN date_dim.d_day_name IS 'Name of the day (e.g., Monday).';
COMMENT ON COLUMN date_dim.d_quarter_name IS 'Name of the quarter (e.g., Q1YYYY).';
COMMENT ON COLUMN date_dim.d_holiday IS 'Flag indicating if the date is a holiday (Y/N).';
COMMENT ON COLUMN date_dim.d_weekend IS 'Flag indicating if the date is a weekend (Y/N).';
COMMENT ON COLUMN date_dim.d_following_holiday IS 'Flag indicating if the date is following a holiday (Y/N).';
COMMENT ON COLUMN date_dim.d_first_dom IS 'First day of the month surrogate key.';
COMMENT ON COLUMN date_dim.d_last_dom IS 'Last day of the month surrogate key.';
COMMENT ON COLUMN date_dim.d_same_day_ly IS 'Surrogate key for the same day last year.';
COMMENT ON COLUMN date_dim.d_same_day_lq IS 'Surrogate key for the same day last quarter.';
COMMENT ON COLUMN date_dim.d_current_day IS 'Flag if this date is the current day (Y/N).';
COMMENT ON COLUMN date_dim.d_current_week IS 'Flag if this date is in the current week (Y/N).';
COMMENT ON COLUMN date_dim.d_current_month IS 'Flag if this date is in the current month (Y/N).';
COMMENT ON COLUMN date_dim.d_current_quarter IS 'Flag if this date is in the current quarter (Y/N).';
COMMENT ON COLUMN date_dim.d_current_year IS 'Flag if this date is in the current year (Y/N).';

-- Comments for table: warehouse
COMMENT ON TABLE warehouse IS 'Stores warehouse information.';
COMMENT ON COLUMN warehouse.w_warehouse_sk IS 'Surrogate key for warehouse.';
COMMENT ON COLUMN warehouse.w_warehouse_id IS 'Business key for warehouse.';
COMMENT ON COLUMN warehouse.w_warehouse_name IS 'Name of the warehouse.';
COMMENT ON COLUMN warehouse.w_warehouse_sq_ft IS 'Square footage of the warehouse.';
COMMENT ON COLUMN warehouse.w_street_number IS 'Street number of the warehouse address.';
COMMENT ON COLUMN warehouse.w_street_name IS 'Street name of the warehouse address.';
COMMENT ON COLUMN warehouse.w_street_type IS 'Street type of the warehouse address.';
COMMENT ON COLUMN warehouse.w_suite_number IS 'Suite number of the warehouse address.';
COMMENT ON COLUMN warehouse.w_city IS 'City of the warehouse.';
COMMENT ON COLUMN warehouse.w_county IS 'County of the warehouse.';
COMMENT ON COLUMN warehouse.w_state IS 'State of the warehouse.';
COMMENT ON COLUMN warehouse.w_zip IS 'ZIP code of the warehouse.';
COMMENT ON COLUMN warehouse.w_country IS 'Country of the warehouse.';
COMMENT ON COLUMN warehouse.w_gmt_offset IS 'GMT offset for the warehouse location.';

-- Comments for table: ship_mode
COMMENT ON TABLE ship_mode IS 'Dimension table for shipping modes.';
COMMENT ON COLUMN ship_mode.sm_ship_mode_sk IS 'Surrogate key for shipping mode.';
COMMENT ON COLUMN ship_mode.sm_ship_mode_id IS 'Business key for shipping mode.';
COMMENT ON COLUMN ship_mode.sm_type IS 'Type of shipping mode (e.g., AIR, TRUCK, MAIL).';
COMMENT ON COLUMN ship_mode.sm_code IS 'Shipping mode code.';
COMMENT ON COLUMN ship_mode.sm_carrier IS 'Shipping carrier name.';
COMMENT ON COLUMN ship_mode.sm_contract IS 'Contract ID with the carrier.';

-- Comments for table: time_dim
COMMENT ON TABLE time_dim IS 'Dimension table for time of day.';
COMMENT ON COLUMN time_dim.t_time_sk IS 'Surrogate key for time.';
COMMENT ON COLUMN time_dim.t_time_id IS 'Business key for time (e.g., HHMMSS).';
COMMENT ON COLUMN time_dim.t_time IS 'Time represented as seconds since midnight.';
COMMENT ON COLUMN time_dim.t_hour IS 'Hour of the day (0-23).';
COMMENT ON COLUMN time_dim.t_minute IS 'Minute of the hour (0-59).';
COMMENT ON COLUMN time_dim.t_second IS 'Second of the minute (0-59).';
COMMENT ON COLUMN time_dim.t_am_pm IS 'AM/PM indicator.';
COMMENT ON COLUMN time_dim.t_shift IS 'Work shift (e.g., Morning, Afternoon, Evening).';
COMMENT ON COLUMN time_dim.t_sub_shift IS 'Sub-shift within a shift.';
COMMENT ON COLUMN time_dim.t_meal_time IS 'Meal time indicator (e.g., Breakfast, Lunch, Dinner).';

-- Comments for table: reason
COMMENT ON TABLE reason IS 'Dimension table for return reasons.';
COMMENT ON COLUMN reason.r_reason_sk IS 'Surrogate key for reason.';
COMMENT ON COLUMN reason.r_reason_id IS 'Business key for reason.';
COMMENT ON COLUMN reason.r_reason_desc IS 'Description of the reason for return.';

-- Comments for table: income_band
COMMENT ON TABLE income_band IS 'Dimension table for income bands.';
COMMENT ON COLUMN income_band.ib_income_band_sk IS 'Surrogate key for income band.';
COMMENT ON COLUMN income_band.ib_lower_bound IS 'Lower bound of the income band.';
COMMENT ON COLUMN income_band.ib_upper_bound IS 'Upper bound of the income band.';

-- Comments for table: item
COMMENT ON TABLE item IS 'Dimension table for items or products.';
COMMENT ON COLUMN item.i_item_sk IS 'Surrogate key for item.';
COMMENT ON COLUMN item.i_item_id IS 'Business key for item (SKU).';
COMMENT ON COLUMN item.i_rec_start_date IS 'Record start date (for versioning).';
COMMENT ON COLUMN item.i_rec_end_date IS 'Record end date (for versioning).';
COMMENT ON COLUMN item.i_item_desc IS 'Description of the item.';
COMMENT ON COLUMN item.i_current_price IS 'Current selling price of the item.';
COMMENT ON COLUMN item.i_wholesale_cost IS 'Wholesale cost of the item.';
COMMENT ON COLUMN item.i_brand_id IS 'Identifier for the brand.';
COMMENT ON COLUMN item.i_brand IS 'Brand name of the item.';
COMMENT ON COLUMN item.i_class_id IS 'Identifier for the item class.';
COMMENT ON COLUMN item.i_class IS 'Class name of the item.';
COMMENT ON COLUMN item.i_category_id IS 'Identifier for the item category.';
COMMENT ON COLUMN item.i_category IS 'Category name of the item.';
COMMENT ON COLUMN item.i_manufact_id IS 'Identifier for the manufacturer.';
COMMENT ON COLUMN item.i_manufact IS 'Manufacturer name of the item.';
COMMENT ON COLUMN item.i_size IS 'Size of the item (e.g., Small, Medium, Large, specific units).';
COMMENT ON COLUMN item.i_formulation IS 'Formulation or style of the item.';
COMMENT ON COLUMN item.i_color IS 'Color of the item.';
COMMENT ON COLUMN item.i_units IS 'Units of the item (e.g., Each, Lb, Oz).';
COMMENT ON COLUMN item.i_container IS 'Container type of the item (e.g., Box, Bag, Bottle).';
COMMENT ON COLUMN item.i_manager_id IS 'Identifier for the product manager responsible for this item.';
COMMENT ON COLUMN item.i_product_name IS 'Product name of the item.';

-- Comments for table: store
COMMENT ON TABLE store IS 'Dimension table for physical stores.';
COMMENT ON COLUMN store.s_store_sk IS 'Surrogate key for store.';
COMMENT ON COLUMN store.s_store_id IS 'Business key for store.';
COMMENT ON COLUMN store.s_rec_start_date IS 'Record start date (for versioning).';
COMMENT ON COLUMN store.s_rec_end_date IS 'Record end date (for versioning).';
COMMENT ON COLUMN store.s_closed_date_sk IS 'Surrogate key for the date the store was closed (if applicable).';
COMMENT ON COLUMN store.s_store_name IS 'Name of the store.';
COMMENT ON COLUMN store.s_number_employees IS 'Number of employees in the store.';
COMMENT ON COLUMN store.s_floor_space IS 'Floor space of the store in square feet.';
COMMENT ON COLUMN store.s_hours IS 'Operating hours of the store (e.g., 8AM-10PM).';
COMMENT ON COLUMN store.s_manager IS 'Name of the store manager.';
COMMENT ON COLUMN store.s_market_id IS 'Identifier for the market this store belongs to.';
COMMENT ON COLUMN store.s_geography_class IS 'Geographical classification of the store location.';
COMMENT ON COLUMN store.s_market_desc IS 'Description of the market.';
COMMENT ON COLUMN store.s_market_manager IS 'Name of the market manager.';
COMMENT ON COLUMN store.s_division_id IS 'Identifier for the division this store belongs to.';
COMMENT ON COLUMN store.s_division_name IS 'Name of the division.';
COMMENT ON COLUMN store.s_company_id IS 'Identifier for the company this store belongs to.';
COMMENT ON COLUMN store.s_company_name IS 'Name of the company.';
COMMENT ON COLUMN store.s_street_number IS 'Street number of the store address.';
COMMENT ON COLUMN store.s_street_name IS 'Street name of the store address.';
COMMENT ON COLUMN store.s_street_type IS 'Street type of the store address.';
COMMENT ON COLUMN store.s_suite_number IS 'Suite number of the store address.';
COMMENT ON COLUMN store.s_city IS 'City where the store is located.';
COMMENT ON COLUMN store.s_county IS 'County where the store is located.';
COMMENT ON COLUMN store.s_state IS 'State where the store is located.';
COMMENT ON COLUMN store.s_zip IS 'ZIP code of the store.';
COMMENT ON COLUMN store.s_country IS 'Country where the store is located.';
COMMENT ON COLUMN store.s_gmt_offset IS 'GMT offset for the store location.';
COMMENT ON COLUMN store.s_tax_precentage IS 'Sales tax percentage applicable at the store (typo in DDL: precentage).';

-- Comments for table: call_center
COMMENT ON TABLE call_center IS 'Dimension table for call centers.';
COMMENT ON COLUMN call_center.cc_call_center_sk IS 'Surrogate key for call center.';
COMMENT ON COLUMN call_center.cc_call_center_id IS 'Business key for call center.';
COMMENT ON COLUMN call_center.cc_rec_start_date IS 'Record start date.';
COMMENT ON COLUMN call_center.cc_rec_end_date IS 'Record end date.';
COMMENT ON COLUMN call_center.cc_closed_date_sk IS 'Surrogate key for the date call center was closed.';
COMMENT ON COLUMN call_center.cc_open_date_sk IS 'Surrogate key for the date call center was opened.';
COMMENT ON COLUMN call_center.cc_name IS 'Name of the call center.';
COMMENT ON COLUMN call_center.cc_class IS 'Class or type of the call center.';
COMMENT ON COLUMN call_center.cc_employees IS 'Number of employees in the call center.';
COMMENT ON COLUMN call_center.cc_sq_ft IS 'Square footage of the call center.';
COMMENT ON COLUMN call_center.cc_hours IS 'Operating hours of the call center.';
COMMENT ON COLUMN call_center.cc_manager IS 'Name of the call center manager.';
COMMENT ON COLUMN call_center.cc_mkt_id IS 'Market ID associated with the call center.';
COMMENT ON COLUMN call_center.cc_mkt_class IS 'Market class string.';
COMMENT ON COLUMN call_center.cc_mkt_desc IS 'Market description.';
COMMENT ON COLUMN call_center.cc_market_manager IS 'Name of the market manager.';
COMMENT ON COLUMN call_center.cc_division IS 'Division ID.';
COMMENT ON COLUMN call_center.cc_division_name IS 'Division name.';
COMMENT ON COLUMN call_center.cc_company IS 'Company ID.';
COMMENT ON COLUMN call_center.cc_company_name IS 'Company name.';
COMMENT ON COLUMN call_center.cc_street_number IS 'Street number of the call center address.';
COMMENT ON COLUMN call_center.cc_street_name IS 'Street name of the call center address.';
COMMENT ON COLUMN call_center.cc_street_type IS 'Street type of the call center address.';
COMMENT ON COLUMN call_center.cc_suite_number IS 'Suite number of the call center address.';
COMMENT ON COLUMN call_center.cc_city IS 'City of the call center.';
COMMENT ON COLUMN call_center.cc_county IS 'County of the call center.';
COMMENT ON COLUMN call_center.cc_state IS 'State of the call center.';
COMMENT ON COLUMN call_center.cc_zip IS 'ZIP code of the call center.';
COMMENT ON COLUMN call_center.cc_country IS 'Country of the call center.';
COMMENT ON COLUMN call_center.cc_gmt_offset IS 'GMT offset for the call center location.';
COMMENT ON COLUMN call_center.cc_tax_percentage IS 'Tax percentage applicable through this call center.';

-- Comments for table: customer
COMMENT ON TABLE customer IS 'Dimension table for customers.';
COMMENT ON COLUMN customer.c_customer_sk IS 'Surrogate key for customer.';
COMMENT ON COLUMN customer.c_customer_id IS 'Business key for customer.';
COMMENT ON COLUMN customer.c_current_cdemo_sk IS 'Foreign key to customer_demographics for current demographics.';
COMMENT ON COLUMN customer.c_current_hdemo_sk IS 'Foreign key to household_demographics for current household demographics.';
COMMENT ON COLUMN customer.c_current_addr_sk IS 'Foreign key to customer_address for current address.';
COMMENT ON COLUMN customer.c_first_shipto_date_sk IS 'Surrogate key for the date of the first shipment to the customer.';
COMMENT ON COLUMN customer.c_first_sales_date_sk IS 'Surrogate key for the date of the first sale to the customer.';
COMMENT ON COLUMN customer.c_salutation IS 'Salutation (e.g., Mr., Ms., Dr.).';
COMMENT ON COLUMN customer.c_first_name IS 'First name of the customer.';
COMMENT ON COLUMN customer.c_last_name IS 'Last name of the customer.';
COMMENT ON COLUMN customer.c_preferred_cust_flag IS 'Flag indicating if the customer is preferred (Y/N).';
COMMENT ON COLUMN customer.c_birth_day IS 'Day of birth of the customer.';
COMMENT ON COLUMN customer.c_birth_month IS 'Month of birth of the customer.';
COMMENT ON COLUMN customer.c_birth_year IS 'Year of birth of the customer.';
COMMENT ON COLUMN customer.c_birth_country IS 'Country of birth of the customer.';
COMMENT ON COLUMN customer.c_login IS 'Login username for the customer.';
COMMENT ON COLUMN customer.c_email_address IS 'Email address of the customer.';
COMMENT ON COLUMN customer.c_last_review_date IS 'Date of the last review or survey by the customer (YYYYMMDD).'; -- DDL shows char(10), likely date as string

-- Comments for table: web_site
COMMENT ON TABLE web_site IS 'Dimension table for web sites.';
COMMENT ON COLUMN web_site.web_site_sk IS 'Surrogate key for web site.';
COMMENT ON COLUMN web_site.web_site_id IS 'Business key for web site.';
COMMENT ON COLUMN web_site.web_rec_start_date IS 'Record start date.';
COMMENT ON COLUMN web_site.web_rec_end_date IS 'Record end date.';
COMMENT ON COLUMN web_site.web_name IS 'Name of the web site.';
COMMENT ON COLUMN web_site.web_open_date_sk IS 'Surrogate key for the date web site was opened.';
COMMENT ON COLUMN web_site.web_close_date_sk IS 'Surrogate key for the date web site was closed.';
COMMENT ON COLUMN web_site.web_class IS 'Class or type of the web site.';
COMMENT ON COLUMN web_site.web_manager IS 'Name of the web site manager.';
COMMENT ON COLUMN web_site.web_mkt_id IS 'Market ID associated with the web site.';
COMMENT ON COLUMN web_site.web_mkt_class IS 'Market class string.';
COMMENT ON COLUMN web_site.web_mkt_desc IS 'Market description.';
COMMENT ON COLUMN web_site.web_market_manager IS 'Name of the market manager.';
COMMENT ON COLUMN web_site.web_company_id IS 'Company ID.';
COMMENT ON COLUMN web_site.web_company_name IS 'Company name.';
COMMENT ON COLUMN web_site.web_street_number IS 'Street number of the web site''s physical address (if any).';
COMMENT ON COLUMN web_site.web_street_name IS 'Street name of the web site''s physical address.';
COMMENT ON COLUMN web_site.web_street_type IS 'Street type of the web site''s physical address.';
COMMENT ON COLUMN web_site.web_suite_number IS 'Suite number of the web site''s physical address.';
COMMENT ON COLUMN web_site.web_city IS 'City of the web site''s physical address.';
COMMENT ON COLUMN web_site.web_county IS 'County of the web site''s physical address.';
COMMENT ON COLUMN web_site.web_state IS 'State of the web site''s physical address.';
COMMENT ON COLUMN web_site.web_zip IS 'ZIP code of the web site''s physical address.';
COMMENT ON COLUMN web_site.web_country IS 'Country of the web site''s physical address.';
COMMENT ON COLUMN web_site.web_gmt_offset IS 'GMT offset for the web site server location.';
COMMENT ON COLUMN web_site.web_tax_percentage IS 'Tax percentage applicable for sales through this web site.';

-- Comments for table: store_returns
COMMENT ON TABLE store_returns IS 'Fact table for store returns.';
COMMENT ON COLUMN store_returns.sr_returned_date_sk IS 'Surrogate key for the date of return.';
COMMENT ON COLUMN store_returns.sr_return_time_sk IS 'Surrogate key for the time of return.';
COMMENT ON COLUMN store_returns.sr_item_sk IS 'Foreign key to the item table for the returned item.';
COMMENT ON COLUMN store_returns.sr_customer_sk IS 'Foreign key to the customer table for the returning customer.';
COMMENT ON COLUMN store_returns.sr_cdemo_sk IS 'Foreign key to customer_demographics at the time of return.';
COMMENT ON COLUMN store_returns.sr_hdemo_sk IS 'Foreign key to household_demographics at the time of return.';
COMMENT ON COLUMN store_returns.sr_addr_sk IS 'Foreign key to customer_address at the time of return.';
COMMENT ON COLUMN store_returns.sr_store_sk IS 'Foreign key to the store table where the item was returned.';
COMMENT ON COLUMN store_returns.sr_reason_sk IS 'Foreign key to the reason table for the return reason.';
COMMENT ON COLUMN store_returns.sr_ticket_number IS 'Ticket number of the original sales transaction.';
COMMENT ON COLUMN store_returns.sr_return_quantity IS 'Quantity of the item returned.';
COMMENT ON COLUMN store_returns.sr_return_amt IS 'Return amount before tax.';
COMMENT ON COLUMN store_returns.sr_return_tax IS 'Tax amount on the return.';
COMMENT ON COLUMN store_returns.sr_return_amt_inc_tax IS 'Return amount including tax.';
COMMENT ON COLUMN store_returns.sr_fee IS 'Restocking or return fee.';
COMMENT ON COLUMN store_returns.sr_return_ship_cost IS 'Shipping cost associated with the return (if any).';
COMMENT ON COLUMN store_returns.sr_refunded_cash IS 'Amount refunded as cash.';
COMMENT ON COLUMN store_returns.sr_reversed_charge IS 'Amount refunded via reversing a charge.';
COMMENT ON COLUMN store_returns.sr_store_credit IS 'Amount refunded as store credit.';
COMMENT ON COLUMN store_returns.sr_net_loss IS 'Net loss incurred from the return.';

-- Comments for table: household_demographics
COMMENT ON TABLE household_demographics IS 'Stores household demographic information.';
COMMENT ON COLUMN household_demographics.hd_demo_sk IS 'Surrogate key for household demographics.';
COMMENT ON COLUMN household_demographics.hd_income_band_sk IS 'Foreign key to the income_band table.';
COMMENT ON COLUMN household_demographics.hd_buy_potential IS 'Buying potential of the household (e.g., >10000, 5001-10000).';
COMMENT ON COLUMN household_demographics.hd_dep_count IS 'Number of dependents in the household.';
COMMENT ON COLUMN household_demographics.hd_vehicle_count IS 'Number of vehicles owned by the household.';

-- Comments for table: web_page
COMMENT ON TABLE web_page IS 'Dimension table for web pages.';
COMMENT ON COLUMN web_page.wp_web_page_sk IS 'Surrogate key for web page.';
COMMENT ON COLUMN web_page.wp_web_page_id IS 'Business key for web page.';
COMMENT ON COLUMN web_page.wp_rec_start_date IS 'Record start date.';
COMMENT ON COLUMN web_page.wp_rec_end_date IS 'Record end date.';
COMMENT ON COLUMN web_page.wp_creation_date_sk IS 'Surrogate key for the date web page was created.';
COMMENT ON COLUMN web_page.wp_access_date_sk IS 'Surrogate key for the last access date of the web page metadata.';
COMMENT ON COLUMN web_page.wp_autogen_flag IS 'Flag indicating if the page is auto-generated (Y/N).';
COMMENT ON COLUMN web_page.wp_customer_sk IS 'Associated customer SK if page is customer-specific (e.g., account page).';
COMMENT ON COLUMN web_page.wp_url IS 'URL of the web page.';
COMMENT ON COLUMN web_page.wp_type IS 'Type or category of the web page (e.g., product, review, search).';
COMMENT ON COLUMN web_page.wp_char_count IS 'Character count of the page content.';
COMMENT ON COLUMN web_page.wp_link_count IS 'Number of links on the page.';
COMMENT ON COLUMN web_page.wp_image_count IS 'Number of images on the page.';
COMMENT ON COLUMN web_page.wp_max_ad_count IS 'Maximum number of ads that can be displayed on the page.';

-- Comments for table: promotion
COMMENT ON TABLE promotion IS 'Dimension table for promotions.';
COMMENT ON COLUMN promotion.p_promo_sk IS 'Surrogate key for promotion.';
COMMENT ON COLUMN promotion.p_promo_id IS 'Business key for promotion.';
COMMENT ON COLUMN promotion.p_start_date_sk IS 'Surrogate key for the promotion start date.';
COMMENT ON COLUMN promotion.p_end_date_sk IS 'Surrogate key for the promotion end date.';
COMMENT ON COLUMN promotion.p_item_sk IS 'Foreign key to item table if promotion is item-specific.';
COMMENT ON COLUMN promotion.p_cost IS 'Cost of running the promotion.';
COMMENT ON COLUMN promotion.p_response_target IS 'Target response for the promotion (1=likely, 0=unlikely to respond to this promo type).';
COMMENT ON COLUMN promotion.p_promo_name IS 'Name of the promotion (e.g., Super Sunday Sale).';
COMMENT ON COLUMN promotion.p_channel_dmail IS 'Flag if promotion uses direct mail channel (Y/N).';
COMMENT ON COLUMN promotion.p_channel_email IS 'Flag if promotion uses email channel (Y/N).';
COMMENT ON COLUMN promotion.p_channel_catalog IS 'Flag if promotion uses catalog channel (Y/N).';
COMMENT ON COLUMN promotion.p_channel_tv IS 'Flag if promotion uses TV channel (Y/N).';
COMMENT ON COLUMN promotion.p_channel_radio IS 'Flag if promotion uses radio channel (Y/N).';
COMMENT ON COLUMN promotion.p_channel_press IS 'Flag if promotion uses press/print channel (Y/N).';
COMMENT ON COLUMN promotion.p_channel_event IS 'Flag if promotion uses event channel (Y/N).';
COMMENT ON COLUMN promotion.p_channel_demo IS 'Flag if promotion uses in-store demo channel (Y/N).';
COMMENT ON COLUMN promotion.p_channel_details IS 'Details about channels used.';
COMMENT ON COLUMN promotion.p_purpose IS 'Purpose of the promotion (e.g., clearance, brand building).';
COMMENT ON COLUMN promotion.p_discount_active IS 'Flag if promotion involves an active discount (Y/N).';

-- Comments for table: catalog_page
COMMENT ON TABLE catalog_page IS 'Dimension table for catalog pages.';
COMMENT ON COLUMN catalog_page.cp_catalog_page_sk IS 'Surrogate key for catalog page.';
COMMENT ON COLUMN catalog_page.cp_catalog_page_id IS 'Business key for catalog page.';
COMMENT ON COLUMN catalog_page.cp_start_date_sk IS 'Surrogate key for the date this catalog page becomes active.';
COMMENT ON COLUMN catalog_page.cp_end_date_sk IS 'Surrogate key for the date this catalog page expires.';
COMMENT ON COLUMN catalog_page.cp_department IS 'Department featured on the catalog page.';
COMMENT ON COLUMN catalog_page.cp_catalog_number IS 'Catalog number.';
COMMENT ON COLUMN catalog_page.cp_catalog_page_number IS 'Page number within the catalog.';
COMMENT ON COLUMN catalog_page.cp_description IS 'Description of the catalog page content.';
COMMENT ON COLUMN catalog_page.cp_type IS 'Type of catalog page (e.g., electronics, apparel, seasonal).';

-- Comments for table: inventory
COMMENT ON TABLE inventory IS 'Fact table for item inventory levels.';
COMMENT ON COLUMN inventory.inv_date_sk IS 'Surrogate key for the date of inventory snapshot.';
COMMENT ON COLUMN inventory.inv_item_sk IS 'Foreign key to the item table.';
COMMENT ON COLUMN inventory.inv_warehouse_sk IS 'Foreign key to the warehouse table.';
COMMENT ON COLUMN inventory.inv_quantity_on_hand IS 'Quantity of the item on hand in the warehouse on that date.';

-- Comments for table: catalog_returns
COMMENT ON TABLE catalog_returns IS 'Fact table for catalog returns.';
COMMENT ON COLUMN catalog_returns.cr_returned_date_sk IS 'Surrogate key for the date of return.';
COMMENT ON COLUMN catalog_returns.cr_returned_time_sk IS 'Surrogate key for the time of return.';
COMMENT ON COLUMN catalog_returns.cr_item_sk IS 'Foreign key to item table for the returned item.';
COMMENT ON COLUMN catalog_returns.cr_refunded_customer_sk IS 'FK to customer who received refund (may be different from returning).';
COMMENT ON COLUMN catalog_returns.cr_refunded_cdemo_sk IS 'FK to customer_demographics for refunded customer.';
COMMENT ON COLUMN catalog_returns.cr_refunded_hdemo_sk IS 'FK to household_demographics for refunded customer.';
COMMENT ON COLUMN catalog_returns.cr_refunded_addr_sk IS 'FK to customer_address for refunded customer.';
COMMENT ON COLUMN catalog_returns.cr_returning_customer_sk IS 'FK to customer who initiated return.';
COMMENT ON COLUMN catalog_returns.cr_returning_cdemo_sk IS 'FK to customer_demographics for returning customer.';
COMMENT ON COLUMN catalog_returns.cr_returning_hdemo_sk IS 'FK to household_demographics for returning customer.';
COMMENT ON COLUMN catalog_returns.cr_returning_addr_sk IS 'FK to customer_address for returning customer.';
COMMENT ON COLUMN catalog_returns.cr_call_center_sk IS 'FK to call_center if return was processed via call center.';
COMMENT ON COLUMN catalog_returns.cr_catalog_page_sk IS 'FK to catalog_page from which the item was originally ordered.';
COMMENT ON COLUMN catalog_returns.cr_ship_mode_sk IS 'FK to ship_mode used for returning item.';
COMMENT ON COLUMN catalog_returns.cr_warehouse_sk IS 'FK to warehouse where item was returned.';
COMMENT ON COLUMN catalog_returns.cr_reason_sk IS 'FK to reason for the return.';
COMMENT ON COLUMN catalog_returns.cr_order_number IS 'Original catalog sales order number.';
COMMENT ON COLUMN catalog_returns.cr_return_quantity IS 'Quantity of the item returned.';
COMMENT ON COLUMN catalog_returns.cr_return_amount IS 'Return amount before tax.';
COMMENT ON COLUMN catalog_returns.cr_return_tax IS 'Tax amount on the return.';
COMMENT ON COLUMN catalog_returns.cr_return_amt_inc_tax IS 'Return amount including tax.';
COMMENT ON COLUMN catalog_returns.cr_fee IS 'Restocking or return fee.';
COMMENT ON COLUMN catalog_returns.cr_return_ship_cost IS 'Shipping cost for returning the item.';
COMMENT ON COLUMN catalog_returns.cr_refunded_cash IS 'Amount refunded as cash.';
COMMENT ON COLUMN catalog_returns.cr_reversed_charge IS 'Amount refunded via reversing a charge.';
COMMENT ON COLUMN catalog_returns.cr_store_credit IS 'Amount refunded as store credit.';
COMMENT ON COLUMN catalog_returns.cr_net_loss IS 'Net loss incurred from this catalog return.';

-- Comments for table: web_returns
COMMENT ON TABLE web_returns IS 'Fact table for web returns.';
COMMENT ON COLUMN web_returns.wr_returned_date_sk IS 'Surrogate key for the date of return.';
COMMENT ON COLUMN web_returns.wr_returned_time_sk IS 'Surrogate key for the time of return.';
COMMENT ON COLUMN web_returns.wr_item_sk IS 'Foreign key to item table for the returned item.';
COMMENT ON COLUMN web_returns.wr_refunded_customer_sk IS 'FK to customer who received refund.';
COMMENT ON COLUMN web_returns.wr_refunded_cdemo_sk IS 'FK to customer_demographics for refunded customer.';
COMMENT ON COLUMN web_returns.wr_refunded_hdemo_sk IS 'FK to household_demographics for refunded customer.';
COMMENT ON COLUMN web_returns.wr_refunded_addr_sk IS 'FK to customer_address for refunded customer.';
COMMENT ON COLUMN web_returns.wr_returning_customer_sk IS 'FK to customer who initiated return.';
COMMENT ON COLUMN web_returns.wr_returning_cdemo_sk IS 'FK to customer_demographics for returning customer.';
COMMENT ON COLUMN web_returns.wr_returning_hdemo_sk IS 'FK to household_demographics for returning customer.';
COMMENT ON COLUMN web_returns.wr_returning_addr_sk IS 'FK to customer_address for returning customer.';
COMMENT ON COLUMN web_returns.wr_web_page_sk IS 'FK to web_page from which the item was originally ordered/returned.';
COMMENT ON COLUMN web_returns.wr_reason_sk IS 'FK to reason for the return.';
COMMENT ON COLUMN web_returns.wr_order_number IS 'Original web sales order number.';
COMMENT ON COLUMN web_returns.wr_return_quantity IS 'Quantity of the item returned.';
COMMENT ON COLUMN web_returns.wr_return_amt IS 'Return amount before tax.';
COMMENT ON COLUMN web_returns.wr_return_tax IS 'Tax amount on the return.';
COMMENT ON COLUMN web_returns.wr_return_amt_inc_tax IS 'Return amount including tax.';
COMMENT ON COLUMN web_returns.wr_fee IS 'Restocking or return fee.';
COMMENT ON COLUMN web_returns.wr_return_ship_cost IS 'Shipping cost for returning the item.';
COMMENT ON COLUMN web_returns.wr_refunded_cash IS 'Amount refunded as cash.';
COMMENT ON COLUMN web_returns.wr_reversed_charge IS 'Amount refunded via reversing a charge.';
COMMENT ON COLUMN web_returns.wr_account_credit IS 'Amount refunded as account credit.';
COMMENT ON COLUMN web_returns.wr_net_loss IS 'Net loss incurred from this web return.';

-- Comments for table: web_sales
COMMENT ON TABLE web_sales IS 'Fact table for web sales.';
COMMENT ON COLUMN web_sales.ws_sold_date_sk IS 'Surrogate key for the date of sale.';
COMMENT ON COLUMN web_sales.ws_sold_time_sk IS 'Surrogate key for the time of sale.';
COMMENT ON COLUMN web_sales.ws_ship_date_sk IS 'Surrogate key for the date item was shipped.';
COMMENT ON COLUMN web_sales.ws_item_sk IS 'Foreign key to the item table.';
COMMENT ON COLUMN web_sales.ws_bill_customer_sk IS 'FK to customer for billing.';
COMMENT ON COLUMN web_sales.ws_bill_cdemo_sk IS 'FK to customer_demographics for billing customer.';
COMMENT ON COLUMN web_sales.ws_bill_hdemo_sk IS 'FK to household_demographics for billing customer.';
COMMENT ON COLUMN web_sales.ws_bill_addr_sk IS 'FK to customer_address for billing address.';
COMMENT ON COLUMN web_sales.ws_ship_customer_sk IS 'FK to customer for shipping (can be different from billing).';
COMMENT ON COLUMN web_sales.ws_ship_cdemo_sk IS 'FK to customer_demographics for shipping customer.';
COMMENT ON COLUMN web_sales.ws_ship_hdemo_sk IS 'FK to household_demographics for shipping customer.';
COMMENT ON COLUMN web_sales.ws_ship_addr_sk IS 'FK to customer_address for shipping address.';
COMMENT ON COLUMN web_sales.ws_web_page_sk IS 'FK to web_page from which the sale originated.';
COMMENT ON COLUMN web_sales.ws_web_site_sk IS 'FK to web_site from which the sale originated.';
COMMENT ON COLUMN web_sales.ws_ship_mode_sk IS 'FK to ship_mode used for this sale.';
COMMENT ON COLUMN web_sales.ws_warehouse_sk IS 'FK to warehouse from which item was shipped.';
COMMENT ON COLUMN web_sales.ws_promo_sk IS 'FK to promotion applied to this sale.';
COMMENT ON COLUMN web_sales.ws_order_number IS 'Web sales order number.';
COMMENT ON COLUMN web_sales.ws_quantity IS 'Quantity of the item sold.';
COMMENT ON COLUMN web_sales.ws_wholesale_cost IS 'Wholesale cost of the item(s) sold.';
COMMENT ON COLUMN web_sales.ws_list_price IS 'List price of the item(s) sold.';
COMMENT ON COLUMN web_sales.ws_sales_price IS 'Actual sales price after discounts (excluding tax, shipping).';
COMMENT ON COLUMN web_sales.ws_ext_discount_amt IS 'Extended discount amount for this line item.';
COMMENT ON COLUMN web_sales.ws_ext_sales_price IS 'Extended sales price (quantity * sales_price).';
COMMENT ON COLUMN web_sales.ws_ext_wholesale_cost IS 'Extended wholesale cost (quantity * wholesale_cost).';
COMMENT ON COLUMN web_sales.ws_ext_list_price IS 'Extended list price (quantity * list_price).';
COMMENT ON COLUMN web_sales.ws_ext_tax IS 'Tax amount for this line item.';
COMMENT ON COLUMN web_sales.ws_coupon_amt IS 'Coupon amount applied to this line item.';
COMMENT ON COLUMN web_sales.ws_ext_ship_cost IS 'Shipping cost for this line item.';
COMMENT ON COLUMN web_sales.ws_net_paid IS 'Net amount paid by customer (excluding tax and shipping).';
COMMENT ON COLUMN web_sales.ws_net_paid_inc_tax IS 'Net amount paid by customer (including tax, excluding shipping).';
COMMENT ON COLUMN web_sales.ws_net_paid_inc_ship IS 'Net amount paid by customer (including shipping, excluding tax).';
COMMENT ON COLUMN web_sales.ws_net_paid_inc_ship_tax IS 'Net amount paid by customer (including tax and shipping).';
COMMENT ON COLUMN web_sales.ws_net_profit IS 'Net profit for this line item.';

-- Comments for table: catalog_sales
COMMENT ON TABLE catalog_sales IS 'Fact table for catalog sales.';
COMMENT ON COLUMN catalog_sales.cs_sold_date_sk IS 'Surrogate key for the date of sale.';
COMMENT ON COLUMN catalog_sales.cs_sold_time_sk IS 'Surrogate key for the time of sale.';
COMMENT ON COLUMN catalog_sales.cs_ship_date_sk IS 'Surrogate key for the date item was shipped.';
COMMENT ON COLUMN catalog_sales.cs_bill_customer_sk IS 'FK to customer for billing.';
COMMENT ON COLUMN catalog_sales.cs_bill_cdemo_sk IS 'FK to customer_demographics for billing customer.';
COMMENT ON COLUMN catalog_sales.cs_bill_hdemo_sk IS 'FK to household_demographics for billing customer.';
COMMENT ON COLUMN catalog_sales.cs_bill_addr_sk IS 'FK to customer_address for billing address.';
COMMENT ON COLUMN catalog_sales.cs_ship_customer_sk IS 'FK to customer for shipping.';
COMMENT ON COLUMN catalog_sales.cs_ship_cdemo_sk IS 'FK to customer_demographics for shipping customer.';
COMMENT ON COLUMN catalog_sales.cs_ship_hdemo_sk IS 'FK to household_demographics for shipping customer.';
COMMENT ON COLUMN catalog_sales.cs_ship_addr_sk IS 'FK to customer_address for shipping address.';
COMMENT ON COLUMN catalog_sales.cs_call_center_sk IS 'FK to call_center if sale was processed via call center.';
COMMENT ON COLUMN catalog_sales.cs_catalog_page_sk IS 'FK to catalog_page from which sale originated.';
COMMENT ON COLUMN catalog_sales.cs_ship_mode_sk IS 'FK to ship_mode used.';
COMMENT ON COLUMN catalog_sales.cs_warehouse_sk IS 'FK to warehouse from which item was shipped.';
COMMENT ON COLUMN catalog_sales.cs_item_sk IS 'Foreign key to the item table.';
COMMENT ON COLUMN catalog_sales.cs_promo_sk IS 'FK to promotion applied.';
COMMENT ON COLUMN catalog_sales.cs_order_number IS 'Catalog sales order number.';
COMMENT ON COLUMN catalog_sales.cs_quantity IS 'Quantity of the item sold.';
COMMENT ON COLUMN catalog_sales.cs_wholesale_cost IS 'Wholesale cost.';
COMMENT ON COLUMN catalog_sales.cs_list_price IS 'List price.';
COMMENT ON COLUMN catalog_sales.cs_sales_price IS 'Actual sales price.';
COMMENT ON COLUMN catalog_sales.cs_ext_discount_amt IS 'Extended discount amount.';
COMMENT ON COLUMN catalog_sales.cs_ext_sales_price IS 'Extended sales price.';
COMMENT ON COLUMN catalog_sales.cs_ext_wholesale_cost IS 'Extended wholesale cost.';
COMMENT ON COLUMN catalog_sales.cs_ext_list_price IS 'Extended list price.';
COMMENT ON COLUMN catalog_sales.cs_ext_tax IS 'Tax amount.';
COMMENT ON COLUMN catalog_sales.cs_coupon_amt IS 'Coupon amount.';
COMMENT ON COLUMN catalog_sales.cs_ext_ship_cost IS 'Shipping cost.';
COMMENT ON COLUMN catalog_sales.cs_net_paid IS 'Net paid (excluding tax, shipping).';
COMMENT ON COLUMN catalog_sales.cs_net_paid_inc_tax IS 'Net paid (including tax, excluding shipping).';
COMMENT ON COLUMN catalog_sales.cs_net_paid_inc_ship IS 'Net paid (including shipping, excluding tax).';
COMMENT ON COLUMN catalog_sales.cs_net_paid_inc_ship_tax IS 'Net paid (including tax and shipping).';
COMMENT ON COLUMN catalog_sales.cs_net_profit IS 'Net profit.';

-- Comments for table: store_sales
COMMENT ON TABLE store_sales IS 'Fact table for store sales.';
COMMENT ON COLUMN store_sales.ss_sold_date_sk IS 'Surrogate key for the date of sale.';
COMMENT ON COLUMN store_sales.ss_sold_time_sk IS 'Surrogate key for the time of sale.';
COMMENT ON COLUMN store_sales.ss_item_sk IS 'Foreign key to the item table.';
COMMENT ON COLUMN store_sales.ss_customer_sk IS 'FK to customer who made the purchase.';
COMMENT ON COLUMN store_sales.ss_cdemo_sk IS 'FK to customer_demographics for purchasing customer.';
COMMENT ON COLUMN store_sales.ss_hdemo_sk IS 'FK to household_demographics for purchasing customer.';
COMMENT ON COLUMN store_sales.ss_addr_sk IS 'FK to customer_address for purchasing customer (likely store address for anonymous sales).';
COMMENT ON COLUMN store_sales.ss_store_sk IS 'FK to store where sale occurred.';
COMMENT ON COLUMN store_sales.ss_promo_sk IS 'FK to promotion applied.';
COMMENT ON COLUMN store_sales.ss_ticket_number IS 'Store sales ticket number (unique within a store/day or globally).';
COMMENT ON COLUMN store_sales.ss_quantity IS 'Quantity of the item sold.';
COMMENT ON COLUMN store_sales.ss_wholesale_cost IS 'Wholesale cost.';
COMMENT ON COLUMN store_sales.ss_list_price IS 'List price.';
COMMENT ON COLUMN store_sales.ss_sales_price IS 'Actual sales price.';
COMMENT ON COLUMN store_sales.ss_ext_discount_amt IS 'Extended discount amount.';
COMMENT ON COLUMN store_sales.ss_ext_sales_price IS 'Extended sales price.';
COMMENT ON COLUMN store_sales.ss_ext_wholesale_cost IS 'Extended wholesale cost.';
COMMENT ON COLUMN store_sales.ss_ext_list_price IS 'Extended list price.';
COMMENT ON COLUMN store_sales.ss_ext_tax IS 'Tax amount.';
COMMENT ON COLUMN store_sales.ss_coupon_amt IS 'Coupon amount.';
COMMENT ON COLUMN store_sales.ss_net_paid IS 'Net paid (excluding tax).';
COMMENT ON COLUMN store_sales.ss_net_paid_inc_tax IS 'Net paid (including tax).';
COMMENT ON COLUMN store_sales.ss_net_profit IS 'Net profit.';