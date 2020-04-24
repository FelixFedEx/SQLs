-- 0. 轉移矩陣
 select felixlin.rfm_transfer('2020-03-01', '2020-04-01', $$Proj016_360_mbr_rfm_transfer$$);


-- 1. 合併六個月的分眾等級
select felixlin.rfm_month('2020-04-01', $$Proj016_360_rfm_month$$);



-- 2. 分眾消費明細
select felixlin.rfm_buy_detail('2019-04-01', '2020-04-01', $$Proj016_360_txn_rfm_buy_detail$$);

-- 3. 分眾消費各通路每月的類別消費狀況(rfm_buy_detail2)
select felixlin.rfm_buy_detail2($$Proj016_360_txn_rfm_buy_detail2$$);

-- 4. 分眾消費summary
select felixlin.rfm_buy_summary('2020-04-01', $$Proj016_360_txn_rfm_buy_summary$$);



-- 5. 整理合約表(rfm_contract)
select felixlin.rfm_contract('2020-04-01', $$Proj016_360_txn_rfm_contract$$);

-- 6. 分眾會員profile
select felixlin.rfm_mmb_info('2020-04-01', $$Proj016_360_mbr_rfm_mmb_info$$);




-- 7. 回購會員明細表
select felixlin.rfm_repo_detail('2020-04-01', $$Proj016_360_txn_rfm_repo_detail$$);

-- 8. 分眾各類別回購表
select felixlin.rfm_repo_summary('2020-04-01', $$Proj016_360_txn_rfm_repo_summary$$);




-- 9. 分眾APP點擊 
select felixlin.rfm_app_summary('2020-04-01', $$Proj016_360_txn_rfm_app_summary$$);

-- 10. 分眾eDM點擊
select felixlin.rfm_edm_summary('2020-04-01', $$Proj016_360_txn_rfm_edm_summary$$);





-- 11. 喚醒的購買館別
select felixlin.rfm_wake_summary1('2020-04-01', $$Proj016_360_txn_rfm_wake_summary1$$);

-- 12. 喚醒的分眾人數
select felixlin.rfm_wake_summary2('2020-04-01', $$Proj016_360_txn_rfm_wake_summary2$$);

-- 13. 分眾人數
select felixlin.rfm_segment_stats('2020-04-01', $$Proj016_360_rfm_stats$$);














-- 查為甚麼待喚回、流失會掉到沉睡
with tmp1 as
(
	select 
		member_id
	from felixlin."Proj016_360_rfm_month"
	where "2020-01-01" = '待喚回忠誠' 
		and "2020-02-01" = '沈睡1'
)
select 
	count(distinct member_id) nmmb,
	count(distinct (member_id, date(order_date))) norder, 
	sum(sl_amt) sl_amt,
	
	-- 購買類別(線上階層)
	count(distinct member_id) filter (where on_p_level1 = '電視．影音．相機') as "電視．影音．相機", 
	count(distinct member_id) filter (where on_p_level1 = '生活家電') as "生活家電", 
	count(distinct member_id) filter (where on_p_level1 = '日用．戶外') as "日用．戶外", 
	count(distinct member_id) filter (where on_p_level1 = '穿戴．配件') as "穿戴．配件", 
	count(distinct member_id) filter (where on_p_level1 = '手機．平板') as "手機．平板", 
	count(distinct member_id) filter (where on_p_level1 = '電腦．遊戲．周邊') as "電腦．遊戲．周邊", 
	count(distinct member_id) filter (where on_p_level1 = '生活．美食') as "生活．美食", 
	count(distinct member_id) filter (where on_p_level1 = '保健．保養') as "保健．保養", 
	count(distinct member_id) filter (where on_p_level1 = '車用百貨') as "車用百貨",
	
	-- 手機品牌
	count(distinct member_id) filter (where off_p_level1 = '通訊商品類' and off_p_level4 = 'APPLE') "APPLE",
	count(distinct member_id) filter (where off_p_level1 = '通訊商品類' and off_p_level4 != 'APPLE') "非APPLE",
	count(distinct member_id) filter (where off_p_level1 = '通訊商品類' and off_p_level4 = '小米') "小米",
	count(distinct member_id) filter (where off_p_level1 = '通訊商品類' and off_p_level4 = 'SAMSUNG') "SAMSUNG",
	count(distinct member_id) filter (where off_p_level1 = '通訊商品類' and off_p_level4 = 'LG') "LG",
	count(distinct member_id) filter (where off_p_level1 = '通訊商品類' and off_p_level4 = 'HUAWEI') "HUAWEI",
	count(distinct member_id) filter (where off_p_level1 = '通訊商品類' and off_p_level4 = 'OPPO') "OPPO",
	count(distinct member_id) filter (where off_p_level1 = '通訊商品類' and off_p_level4 = 'ASUS') "ASUS",
	count(distinct member_id) filter (where off_p_level1 = '通訊商品類' and off_p_level4 = 'HTC') "HTC",
	
	-- 特殊消費類型
	count(distinct member_id) filter (where contract = '綁約') "綁約"
from pdata.txn_allchannel_detail
where member_id in (select member_id from tmp1)
	and order_date >= '2019-01-01'
	and order_date < '2019-02-01'
	and p_no not like '9999%'
	and p_no_bz = 'N'
	and cancel_flag = 'N'
	
	
	


-- 分眾人數
select felixlin.rfm_segment_stats(first_date text, output_table_name text);


select 
	count(distinct member_id) filter (where group_name_all = '忠誠') "VIP",
	count(distinct member_id) filter (where group_name_all = '高潛力') "S1",
	count(distinct member_id) filter (where group_name_all = '低潛力') "S2",
	count(distinct member_id) filter (where group_name_all = '待喚回忠誠') "A1",
	count(distinct member_id) filter (where group_name_all = '流失') "A2",
	count(distinct member_id) filter (where group_name_all = '沈睡1') "沈睡1",
	count(distinct member_id) filter (where group_name_all = '沈睡2') "沈睡2",
	count(distinct member_id) filter (where group_name_all = '沈睡3') "沈睡3",
	count(distinct member_id) filter (where group_name_all = '沈睡4') "沈睡4",
	count(distinct member_id) filter (where group_name_all = '沈睡5') "沈睡5",
	count(distinct member_id) filter (where group_name_all = '沈睡6') "沈睡6",
	count(distinct member_id) filter (where group_name_all = '註冊未購1') "註冊未購1", 
	count(distinct member_id) filter (where group_name_all = '註冊未購2') "註冊未購2",
	count(distinct member_id) filter (where group_name_all = '註冊未購3') "註冊未購3",
	count(distinct member_id) filter (where group_name_all = '註冊未購4') "註冊未購4", 
	count(distinct member_id) filter (where group_name_all = '註冊未購5') "註冊未購5",
	count(distinct member_id) filter (where group_name_all = '註冊未購6') "註冊未購6",
	count(distinct member_id) filter (where group_name_all = '註冊未購7') "註冊未購7",
	count(distinct member_id) filter (where group_name_all = '其他') "其他",
	count(distinct member_id) "會員數" -- 1. 人數
from pdata.mbr_rfm_segment
limit 1000
















