-- 計算某兩個月的分眾轉移矩陣(分眾等級)
-- Input: 第一個月, 第二個月, 輸出的表格名稱(預設schema為felixlin)
-- 需先建立table，此function只做insert，若月份已存在output_table則不做任何計算
create or replace function felixlin.rfm_transfer(first_month text, second_month text, output_table_name text)
returns void as $func$
declare 
	_schema text = 'felixlin';
	_query1 text;  
	_m1 text;  
	_m2 text;
	_find text;
	_input_table_name text := 'pdata.mbr_rfm_segment_history';
begin
	-- check if data exists -> existed then exit function
	_query1 := 
	format(
	$f1$
	create table if not exists %1$s."%2$s"
	(
		calc_date date,	group_name_all text, "會員數" integer, "小B" integer, "忠誠" integer,
		"高潛力" integer, "低潛力" integer,	"待喚回忠誠" integer, "流失" integer,
		"沈睡1" integer, "沈睡2" integer, "沈睡3" integer, "沈睡4" integer,	"沈睡5" integer, "沈睡5" integer, "沈睡6" integer,
		"新戶" integer,
		"註冊未購1" integer, "註冊未購2" integer, "註冊未購3" integer, "註冊未購4" integer, "註冊未購5" integer, "註冊未購6" integer, 
		"註冊未購7" integer, "其他" integer, "一次性購物" integer
	);
	$f1$, _schema, output_table_name);
	execute _query1;
	
	execute format($$select 1 from "%1$s"."%2$s" where calc_date = '%3$s' limit 1$$, _schema, output_table_name, second_month) into _find;
	if _find is not null then 
		raise notice '%','所選月份已經在TABLE裡了!!';
		return; 
	end if;

	-- 如果現在月份不在舊表裡，則去新表裡找
	execute format($$select 1 from pdata.mbr_rfm_segment where calc_date = '%1$s' limit 1$$, second_month) into _find;
	if _find is not null then _input_table_name := 'pdata.mbr_rfm_segment'; end if;
	-- query開始
	_query1 := 
	format(
	$f1$	
	insert into %6$s."%4$s"
	(
		with tmp1 as 
		(
			select 
			aa.member_id, 
			aa.calc_date calc_date,  -- 現在月份
			bb.calc_date as calc_date_prev,  -- 前一月份
			-- aa.group_name_all,  -- 現在月份分眾
			case 
				when mpm.distributor_rfm_flag = 'Y' then '小B'
				else aa.group_name_all
			end group_name_all,  -- 現在月份分眾
			case 
				when mpm.distributor_rfm_flag = 'Y' then '小B'
				when mpm.auth_date >= '%2$s' and mpm.auth_date < '%3$s' then '新戶'
				else bb.group_name_all
			end group_name_all_prev,  -- 前一月份分眾
			aa.f_val_all  -- 一次性購買
			from (select * from %1$s where calc_date='%3$s') aa
			left join (select * from pdata.mbr_rfm_segment_history where calc_date='%2$s') bb using (member_id)
			left join pdata.mbr_profile_mart mpm using (member_id)
		), tmp2 as
		(
			select 
				'%3$s'::date calc_date,
				group_name_all, 
				count(distinct member_id) filter (where group_name_all_prev != '其他') "會員數", -- 1. 人數
				count(distinct member_id) filter (where group_name_all_prev = '小B') "小B",
				count(distinct member_id) filter (where group_name_all_prev = '忠誠') "忠誠",
				count(distinct member_id) filter (where group_name_all_prev = '高潛力') "高潛力",
				count(distinct member_id) filter (where group_name_all_prev = '低潛力') "低潛力",
				count(distinct member_id) filter (where group_name_all_prev = '待喚回忠誠') "待喚回忠誠",
				count(distinct member_id) filter (where group_name_all_prev = '流失') "流失",
				count(distinct member_id) filter (where group_name_all_prev = '沈睡1') "沈睡1",
				count(distinct member_id) filter (where group_name_all_prev = '沈睡2') "沈睡2",
				count(distinct member_id) filter (where group_name_all_prev = '沈睡3') "沈睡3",
				count(distinct member_id) filter (where group_name_all_prev = '沈睡4') "沈睡4",
				count(distinct member_id) filter (where group_name_all_prev = '沈睡5') "沈睡5",
				count(distinct member_id) filter (where group_name_all_prev = '沈睡6') "沈睡6",
				count(distinct member_id) filter (where group_name_all_prev = '新戶') "新戶",
				count(distinct member_id) filter (where group_name_all_prev = '註冊未購1') "註冊未購1", 
				count(distinct member_id) filter (where group_name_all_prev = '註冊未購2') "註冊未購2",
				count(distinct member_id) filter (where group_name_all_prev = '註冊未購3') "註冊未購3",
				count(distinct member_id) filter (where group_name_all_prev = '註冊未購4') "註冊未購4", 
				count(distinct member_id) filter (where group_name_all_prev = '註冊未購5') "註冊未購5",
				count(distinct member_id) filter (where group_name_all_prev = '註冊未購6') "註冊未購6",
				count(distinct member_id) filter (where group_name_all_prev = '註冊未購7') "註冊未購7",
				count(distinct member_id) filter (where group_name_all_prev = '其他') "其他",
				count(distinct member_id) filter (where f_val_all = 1) "一次性購物"
			from tmp1
			group by group_name_all
		)
		select tmp2.*
		from tmp2
		left join (values ('小B',1), ('忠誠',2), ('高潛力',3), ('低潛力',4), ('待喚回忠誠',5), ('流失',6), 
							   ('沈睡1',7), ('沈睡2',8), ('沈睡3',9), ('沈睡4',10), ('沈睡5',11), ('沈睡6',12),
							   ('註冊未購1',13), ('註冊未購2',14), ('註冊未購3',15), ('註冊未購4',16), ('註冊未購5',17), 
							   ('註冊未購6',18), ('註冊未購7',19), 
							   ('其他',20)) t(t1,t2) on tmp2.group_name_all = t.t1
		order by t.t2 asc
	);
	$f1$, _input_table_name, first_month, second_month, output_table_name, _m2, _schema);
	execute _query1;
	
	raise notice '%','計算完成!!';
end;
$func$ language plpgsql;


-- 建議分眾轉移矩陣
/*
drop table if exists felixlin."Proj016_360_mbr_rfm_transfer";
create table felixlin."Proj016_360_mbr_rfm_transfer"
(
	calc_date date,	group_name_all text, "會員數" integer, "小B" integer, "忠誠" integer,
	"高潛力" integer, "低潛力" integer,	"待喚回忠誠" integer, "流失" integer,
	"沈睡1" integer, "沈睡2" integer, "沈睡3" integer, "沈睡4" integer, "沈睡5" integer, "沈睡6" integer,
	"新戶" integer,
	"註冊未購1" integer, "註冊未購2" integer, "註冊未購3" integer, "註冊未購4" integer,
	"註冊未購5" integer, "註冊未購6" integer, "註冊未購7" integer, "其他" integer, "一次性購物" integer
);*/








-- 將六個月的分眾合併成一張大表(分眾等級)
-- Input: 最後一個月(自動往前推半年), 輸出的表格名稱(預設schema為felixlin)
-- 不需先建立table，此function會drop已經存在的表，再重新建立一個新表
create or replace function felixlin.rfm_month(input_month text, output_table_name text)
returns void as $func$
declare 
	_schema text = 'felixlin';
	_query1 text;  
	_m0 date;
	_input_table_name text := 'pdata.mbr_rfm_segment_history';
begin
	_m0 = date_trunc('month', date(input_month));
	-- query開始
	_query1 := 
	format(
	$f1$
	drop table if exists %4$s."%3$s";
	create table %4$s."%3$s" as
	(
		with bmmb as
		(
			select member_id from pdata.mbr_profile_mart where distributor_rfm_flag = 'Y'
		)
		select 
			coalesce(aa.member_id, bb.member_id, cc.member_id, dd.member_id, ee.member_id, ff.member_id) as member_id,
			case --07
				when ff.member_id in (select member_id from bmmb) then '小B'
				when ee.auth_date >= '%10$s' and ee.auth_date < '%9$s' then '新戶'
				else ff.group_name_all
			end "%10$s",
			case  --08
				when ee.member_id in (select member_id from bmmb) then '小B'
				when dd.auth_date >= '%9$s' and dd.auth_date < '%8$s' then '新戶'
				else ee.group_name_all
			end "%9$s",
			case  --09
				when dd.member_id in (select member_id from bmmb) then '小B'
				when cc.auth_date >= '%8$s' and cc.auth_date < '%7$s' then '新戶'
				else dd.group_name_all
			end "%8$s",
			case  --10
				when cc.member_id in (select member_id from bmmb) then '小B'
				when bb.auth_date >= '%7$s' and bb.auth_date < '%6$s' then '新戶'
				else cc.group_name_all
			end "%7$s",
			case  --11
				when bb.member_id in (select member_id from bmmb) then '小B'
				when aa.auth_date >= '%6$s' and aa.auth_date < '%5$s' then '新戶'
				else bb.group_name_all
			end "%6$s",
			case  --12
				when aa.member_id in (select member_id from bmmb) then '小B'
				else aa.group_name_all
			end "%5$s"
		from pdata.mbr_rfm_segment aa --12
		full outer join (select member_id, group_name_all, auth_date from pdata.mbr_rfm_segment_history where calc_date = '%6$s') bb using (member_id)  --11
		full outer join (select member_id, group_name_all, auth_date from pdata.mbr_rfm_segment_history where calc_date = '%7$s') cc using (member_id)  --10
		full outer join (select member_id, group_name_all, auth_date from pdata.mbr_rfm_segment_history where calc_date = '%8$s') dd using (member_id)  --09
		full outer join (select member_id, group_name_all, auth_date from pdata.mbr_rfm_segment_history where calc_date = '%9$s') ee using (member_id)  --08
		full outer join (select member_id, group_name_all, auth_date from pdata.mbr_rfm_segment_history where calc_date = '%10$s') ff using (member_id)  --07
	);$f1$, 
		  _input_table_name, input_month, output_table_name, _schema,
	      date(_m0)::text, date(_m0 - interval '1' month)::text, --12
		  date(_m0 - interval '2' month)::text, date(_m0 - interval '3' month)::text, --10
		  date(_m0 - interval '4' month)::text, date(_m0 - interval '5' month)::text);  --08
	execute _query1;
	
	raise notice '%','計算完成!!';
end;
$func$ language plpgsql;






-- 分眾消費明細
-- Input: 第一個月, 第二個月, 輸出的表格名稱(預設schema為felixlin)
-- 不需先建立table，此function會drop已經存在的表，再重新建立一個新表
create or replace function felixlin.rfm_buy_detail(first_month text, second_month text, output_table_name text)
returns void as $func$
declare 
	_schema text = 'felixlin';
	_query1 text;  
	_find text;
	_input_table_name text := 'pdata.txn_allchannel_detail';
	_input_table_name2 text := 'Proj016_360_rfm_month';
begin
	-- query開始
	_query1 := 
	format(
	$f1$
	drop table if exists %5$s."%4$s";
	create table %5$s."%4$s" as
	(
		with tmp0 as
		(
			select
				distinct on (p_no)
				p_no, level1_name
			from pdata.tb_producttree_ec
			where level1_name not in ('SAMSUNG', 'SONYONLINE', '躍獅連鎖藥局', '贈品', 
										'生活工場', 'MEGAKING')
			order by p_no, update_date desc
		
		), tmp as
		(
			select 
				member_id, off_p_level1, level1_name as on_p_level1, aa.p_name, p_no,
				date_trunc('month', order_date) order_date_m,
				date(order_date) order_date,
				sl_key_new,
				case 
					when o2o_category = 'POS' then '門市'
					when o2o_category = '非O2O' then 'EC'
					else 'O2O'
				end channel,
				sl_amt, per_bonus_fee, contract
			from pdata.txn_allchannel_detail aa
			left join tmp0 bb using (p_no)
			where order_date >= '%2$s' 
				and order_date < '%3$s'
				and p_no_bz = 'N'
				and p_no not like '9999%%'
				and member_id is not null
				and member_id != ''
				and cancel_flag = 'N'
				and sl_amt > 0
				and on_p_level1 not in ('SAMSUNG', 'SONYONLINE', '躍獅連鎖藥局', '贈品', 
										'生活工場', 'MEGAKING')
		)
		select tmp.*, group_name_all
		from tmp
		left join (select member_id, "%3$s" as group_name_all from %5$s."%6$s" where "%3$s" is not null and "%3$s" != '新戶') bb using (member_id)
	);
	$f1$, _input_table_name, first_month, second_month, output_table_name, _schema, _input_table_name2);
	execute _query1;
	
	raise notice '%','計算完成!!';
end;
$func$ language plpgsql;






-- 分眾消費各通路每月的類別消費狀況 
-- Input: 輸出的表格名稱(預設schema為felixlin) 
-- 不需先建立table，此function會drop已經存在的表，再重新建立一個新表 
create or replace function felixlin.rfm_buy_detail2(output_table_name text)
returns void as $func$
declare 
	_schema text = 'felixlin';
	_query1 text;  
	_find text;
	_input_table_name text := $$Proj016_360_txn_rfm_buy_detail$$;
begin
	-- query開始
	_query1 := 
	format(
	$f1$
	drop table if exists %3$s."%2$s";
	create table %3$s."%2$s" as
	(
		with tmp3 as
		(
			select
				member_id, order_date_m, 
				'全通路'::text channel, 
				-- 統計
				count(distinct sl_key_new) sl_key_new,
				count(distinct order_date) norder,
				sum(sl_amt) sl_amt,
				sum(per_bonus_fee) per_bonus_fee,
				sum(case when contract='綁約' then 1 else 0 end) "綁約",
				sum(case when contract='空機' then 1 else 0 end) "空機",
				-- 線上階層次數
				count(distinct order_date) filter (where on_p_level1 = '通訊 手機 平板') as "norder_通訊 手機 平板", 
				count(distinct order_date) filter (where on_p_level1 = '穿戴 音訊 配件') as "norder_穿戴 音訊 配件", 
				count(distinct order_date) filter (where on_p_level1 = '家電 影音 清淨') as "norder_家電 影音 清淨", 
				count(distinct order_date) filter (where on_p_level1 = '保健 保養 美體') as "norder_保健 保養 美體", 
				count(distinct order_date) filter (where on_p_level1 = '資訊 遊戲 車用') as "norder_資訊 遊戲 車用", 
				count(distinct order_date) filter (where on_p_level1 = '生活 日用 美食') as "norder_生活 日用 美食", 
				
				-- 線上階層金額
				sum(sl_amt) filter (where on_p_level1 = '通訊 手機 平板') as "amt_通訊 手機 平板", 
				sum(sl_amt) filter (where on_p_level1 = '穿戴 音訊 配件') as "amt_穿戴 音訊 配件", 
				sum(sl_amt) filter (where on_p_level1 = '家電 影音 清淨') as "amt_家電 影音 清淨", 
				sum(sl_amt) filter (where on_p_level1 = '保健 保養 美體') as "amt_保健 保養 美體", 
				sum(sl_amt) filter (where on_p_level1 = '資訊 遊戲 車用') as "amt_資訊 遊戲 車用", 
				sum(sl_amt) filter (where on_p_level1 = '生活 日用 美食') as "amt_生活 日用 美食"
			from %3$s."%1$s"
			group by member_id, order_date_m
		), tmp4 as
		(
			select
				member_id, order_date_m,
				channel, 
				-- 統計
				count(distinct sl_key_new) sl_key_new,
				count(distinct order_date) norder,
				sum(sl_amt) sl_amt,
				sum(per_bonus_fee) per_bonus_fee,
				sum(case when contract='綁約' then 1 else 0 end) "綁約",
				sum(case when contract='空機' then 1 else 0 end) "空機",
				-- 線上階層次數
				count(distinct order_date) filter (where on_p_level1 = '通訊 手機 平板') as "norder_通訊 手機 平板", 
				count(distinct order_date) filter (where on_p_level1 = '穿戴 音訊 配件') as "norder_穿戴 音訊 配件", 
				count(distinct order_date) filter (where on_p_level1 = '家電 影音 清淨') as "norder_家電 影音 清淨", 
				count(distinct order_date) filter (where on_p_level1 = '保健 保養 美體') as "norder_保健 保養 美體", 
				count(distinct order_date) filter (where on_p_level1 = '資訊 遊戲 車用') as "norder_資訊 遊戲 車用", 
				count(distinct order_date) filter (where on_p_level1 = '生活 日用 美食') as "norder_生活 日用 美食", 
				
				-- 線上階層金額
				sum(sl_amt) filter (where on_p_level1 = '通訊 手機 平板') as "amt_通訊 手機 平板", 
				sum(sl_amt) filter (where on_p_level1 = '穿戴 音訊 配件') as "amt_穿戴 音訊 配件", 
				sum(sl_amt) filter (where on_p_level1 = '家電 影音 清淨') as "amt_家電 影音 清淨", 
				sum(sl_amt) filter (where on_p_level1 = '保健 保養 美體') as "amt_保健 保養 美體", 
				sum(sl_amt) filter (where on_p_level1 = '資訊 遊戲 車用') as "amt_資訊 遊戲 車用", 
				sum(sl_amt) filter (where on_p_level1 = '生活 日用 美食') as "amt_生活 日用 美食"
			from %3$s."%1$s"
			group by member_id, order_date_m, channel
		)
		(select * from tmp3)
		union all
		(select * from tmp4)
	);
	$f1$, _input_table_name, output_table_name, _schema);
	execute _query1;
	
	raise notice '%','計算完成!!';
end;
$func$ language plpgsql;






-- 分眾消費summary
-- Input: 輸出的表格名稱(預設schema為felixlin)
-- 不需先建立table，此function會drop已經存在的表，再重新建立一個新表
create or replace function felixlin.rfm_buy_summary(first_date text, output_table_name text)
returns void as $func$
declare 
	_schema text = 'felixlin';
	_query1 text;  
	_input_table_name text := $$Proj016_360_txn_rfm_buy_detail2$$;
	_input_table_name2 text := $$Proj016_360_rfm_month$$;
	_find text;
begin
	-- query開始
	_query1 := 
	format(
	$f1$
	create table if not exists %4$s."%3$s"
	(
		"月份" date, channel text, group_name_all varchar, "會員數" integer, "用點會員數" integer, 
		"用點總額" numeric,	"消費總金額" numeric, "norder_通訊 手機 平板" integer, "norder_穿戴 音訊 配件" integer,
		"norder_家電 影音 清淨" integer, "norder_保健 保養 美體" integer, "norder_資訊 遊戲 車用" integer, 
		"norder_生活 日用 美食" integer, "amt_通訊 手機 平板" numeric, "amt_穿戴 音訊 配件" numeric, 
		"amt_家電 影音 清淨" numeric, "amt_保健 保養 美體" numeric, "amt_資訊 遊戲 車用" numeric, 
		"amt_生活 日用 美食" numeric
	);$f1$, _input_table_name, _input_table_name2, output_table_name, _schema, first_date);
	execute _query1;
	
	execute format($$select 1 from %4$s."%3$s" where "月份" = '%5$s' limit 1$$, _input_table_name, _input_table_name2, output_table_name, _schema, first_date) into _find;
	if _find is not null then 
		raise notice '%','所選月份已經在TABLE裡了!!';
		return; 
	end if;
	
	_query1 := 
	format(
	$f2$
	insert into %4$s."%3$s"
	(
		with tmp as
		(
			select
				'%5$s'::date "月份",
				channel,
				group_name_all,
				count(distinct aa.member_id) "會員數",
				-- 6. 兑點佔比
				count(distinct aa.member_id) filter (where per_bonus_fee > 0) as "用點會員數",
				sum(per_bonus_fee) "用點總額",
				sum(sl_amt) "消費總金額",
				-- 3. 消費偏好
				-- 次數
				count(distinct aa.member_id) filter (where "norder_通訊 手機 平板" > 0) "norder_通訊 手機 平板", 
				count(distinct aa.member_id) filter (where "norder_穿戴 音訊 配件" > 0) "norder_穿戴 音訊 配件", 
				count(distinct aa.member_id) filter (where "norder_家電 影音 清淨" > 0) "norder_家電 影音 清淨", 
				count(distinct aa.member_id) filter (where "norder_保健 保養 美體" > 0) "norder_保健 保養 美體", 
				count(distinct aa.member_id) filter (where "norder_資訊 遊戲 車用" > 0) "norder_資訊 遊戲 車用", 
				count(distinct aa.member_id) filter (where "norder_生活 日用 美食" > 0) "norder_生活 日用 美食", 
						
				-- 金額
				sum("amt_通訊 手機 平板") "amt_通訊 手機 平板",
				sum("amt_穿戴 音訊 配件") "amt_穿戴 音訊 配件",
				sum("amt_家電 影音 清淨") "amt_家電 影音 清淨",
				sum("amt_保健 保養 美體") "amt_保健 保養 美體",
				sum("amt_資訊 遊戲 車用") "amt_資訊 遊戲 車用",
				sum("amt_生活 日用 美食") "amt_生活 日用 美食"
			from %4$s."%1$s" aa
			inner join (select member_id, "%5$s" group_name_all from %4$s."%2$s" where "%5$s" is not null and "%5$s" != '新戶') bb using (member_id)
			group by channel, group_name_all
		)
		select 
			tmp.* 
		from tmp
		left join (values ('小B',1), ('忠誠',2), ('高潛力',3), ('低潛力',4), ('待喚回忠誠',5), ('流失',6), 
						  ('沈睡1',7), ('沈睡2',8), ('沈睡3',9), ('沈睡4',10), ('沈睡5',11), ('沈睡6',12),
						  ('註冊未購1',13), ('註冊未購2',14), ('註冊未購3',15), ('註冊未購4',16), ('註冊未購5',17), 
						  ('註冊未購6',18), ('註冊未購7',19), 
					      ('其他',20)) t(t1,t2) on tmp.group_name_all = t.t1
		--where not exists (select 1 from %4$s."%3$s" tt where tmp."月份" = '%5$s' and tmp.group_name_all = tt.group_name_all)
		order by channel desc, t.t2 asc
	);
	$f2$, _input_table_name, _input_table_name2, output_table_name, _schema, first_date);
	execute _query1;
	
	raise notice '%','計算完成!!';
end;
$func$ language plpgsql;






-- 整理合約表
-- Input: 日期、輸出的表格名稱(預設schema為felixlin)
-- 不需先建立table，此function會drop已經存在的表，再重新建立一個新表
create or replace function felixlin.rfm_contract(first_date text, output_table_name text)
returns void as $func$
declare 
	_schema text = 'felixlin';
	_query1 text;  
	_find text;
	_input_table_name text := $$Proj016_360_rfm_month$$;
begin
	-- query開始
	_query1 := 
	format(
	$f1$
	drop table if exists %3$s."%2$s";
	create table %3$s."%2$s" as
	(
		select 
			aa.member_id,
			group_name_all,
			p_level1,
			cha_end_date,
			case 
				when cha_end_date < '%4$s'::timestamp + interval '3' month then '3個月內'   ----- 改時間
				when cha_end_date < '%4$s'::timestamp + interval '6' month then '3-6個月'   ----- 改時間
				when cha_end_date < '%4$s'::timestamp + interval '9' month then '6-9個月'    ----- 改時間
				when cha_end_date < '%4$s'::timestamp + interval '12' month then '9-12個月'    ----- 改時間
				when cha_end_date > '%4$s'::timestamp + interval '12' month then '12個月以上'  ----- 改時間
				else null
			end exp_date,
			p_level4, p_name,
			sl_date, p_no,
			name, name2, contracttype, period
		from pdata.txn_cha_sale_mart aa
		left join (select member_id, "%4$s" group_name_all from %3$s."%1$s" where "%4$s" is not null and "%4$s" != '新戶') bb using (member_id)
		where cha_end_date > '%4$s'
			and order_date_new >= '%4$s'::date - interval '4' year
			and cancel_flag = 'N'
	);
	$f1$, _input_table_name, output_table_name, _schema, first_date);
	execute _query1;
	
	raise notice '%','計算完成!!';
end;
$func$ language plpgsql;







-- 分眾重會員profile
-- Input: 日期、輸出的表格名稱(預設schema為felixlin)
-- 不需先建立table，此function會drop已經存在的表，再重新建立一個新表
create or replace function felixlin.rfm_mmb_info(first_date text, output_table_name text)
returns void as $func$
declare 
	_schema text = 'felixlin';
	_query1 text;  
	_find text;
	_input_table_name1 text := $$Proj016_360_rfm_month$$;
	_input_table_name2 text := $$Proj016_360_txn_rfm_contract$$;
begin
	-- query開始
	_query1 := 
	format(
	$f1$
	drop table if exists %4$s."%3$s";
	create table %4$s."%3$s" as
	(
		with tmp as
		(
		select 
			aa.group_name_all, 
			-- 基本資料
			count(distinct aa.member_id) "會員數",
			sum(case when sex = 'M' then 1 else 0 end) "男",
			sum(case when sex = 'F' then 1 else 0 end) "女", 
			sum(case when age < 20 then 1 else 0 end) "19歲以下",
			sum(case when age < 30 then 1 else 0 end) "20-29歲",
			sum(case when age < 40 then 1 else 0 end) "30-39歲",
			sum(case when age < 50 then 1 else 0 end) "40-49歲", 
			sum(case when age < 60 then 1 else 0 end) "50-59歲",
			sum(case when age >= 60 then 1 else 0 end) "60歲以上",
			-- 綁約
			count(distinct aa.member_id) filter (where exp_date is not null) as "綁約人數",
			count(distinct aa.member_id) filter (where exp_date = '3個月內') "3個月內到期",
			count(distinct aa.member_id) filter (where exp_date = '6-9個月') as "3-6個月到期",
			count(distinct aa.member_id) filter (where exp_date = '6-9個月') as "6-9個月到期",
			count(distinct aa.member_id) filter (where exp_date = '9-12個月') as "9-12個月到期",
			count(distinct aa.member_id) filter (where exp_date = '12個月以上') as "12個月以上到期",
			-- 綁約家電
			count(distinct aa.member_id) filter (where exp_date is not null and p_level1 = '家電商品類') as "綁約人數_家電",
			count(distinct aa.member_id) filter (where exp_date = '3個月內' and p_level1 = '家電商品類') "3個月內到期_家電",
			count(distinct aa.member_id) filter (where exp_date = '6-9個月' and p_level1 = '家電商品類') as "3-6個月到期_家電",
			count(distinct aa.member_id) filter (where exp_date = '6-9個月' and p_level1 = '家電商品類') as "6-9個月到期_家電",
			count(distinct aa.member_id) filter (where exp_date = '9-12個月' and p_level1 = '家電商品類') as "9-12個月到期_家電",
			count(distinct aa.member_id) filter (where exp_date = '12個月以上' and p_level1 = '家電商品類') as "12個月以上到期_家電",
			-- 綁約手機
			count(distinct aa.member_id) filter (where exp_date is not null and p_level1 = '通訊商品類') as "綁約人數_手機",
			count(distinct aa.member_id) filter (where exp_date = '3個月內' and p_level1 = '通訊商品類') "3個月內到期_手機",
			count(distinct aa.member_id) filter (where exp_date = '6-9個月' and p_level1 = '通訊商品類') as "3-6個月到期_手機",
			count(distinct aa.member_id) filter (where exp_date = '6-9個月' and p_level1 = '通訊商品類') as "6-9個月到期_手機",
			count(distinct aa.member_id) filter (where exp_date = '9-12個月' and p_level1 = '通訊商品類') as "9-12個月到期_手機",
			count(distinct aa.member_id) filter (where exp_date = '12個月以上' and p_level1 = '通訊商品類') as "12個月以上到期_手機",
			-- 綁約平板
			count(distinct aa.member_id) filter (where exp_date is not null and p_level1 = '平板商品類') as "綁約人數_平板",
			count(distinct aa.member_id) filter (where exp_date = '3個月內' and p_level1 = '平板商品類') "3個月內到期_平板",
			count(distinct aa.member_id) filter (where exp_date = '6-9個月' and p_level1 = '平板商品類') as "3-6個月到期_平板",
			count(distinct aa.member_id) filter (where exp_date = '6-9個月' and p_level1 = '平板商品類') as "6-9個月到期_平板",
			count(distinct aa.member_id) filter (where exp_date = '9-12個月' and p_level1 = '平板商品類') as "9-12個月到期_平板",
			count(distinct aa.member_id) filter (where exp_date = '12個月以上' and p_level1 = '平板商品類') as "12個月以上到期_平板",
			-- 綁約應用周邊
			count(distinct aa.member_id) filter (where exp_date is not null and p_level1 = '應用週邊商品類') as "綁約人數_應用週邊",
			count(distinct aa.member_id) filter (where exp_date = '3個月內' and p_level1 = '應用週邊商品類') "3個月內到期_應用週邊",
			count(distinct aa.member_id) filter (where exp_date = '6-9個月' and p_level1 = '應用週邊商品類') as "3-6個月到期_應用週邊",
			count(distinct aa.member_id) filter (where exp_date = '6-9個月' and p_level1 = '應用週邊商品類') as "6-9個月到期_應用週邊",
			count(distinct aa.member_id) filter (where exp_date = '9-12個月' and p_level1 = '應用週邊商品類') as "9-12個月到期_應用週邊",
			count(distinct aa.member_id) filter (where exp_date = '12個月以上' and p_level1 = '應用週邊商品類') as "12個月以上到期_應用週邊",
			-- 綁約資訊類
			count(distinct aa.member_id) filter (where exp_date is not null and p_level1 = '資訊商品類') as "綁約人數_資訊",
			count(distinct aa.member_id) filter (where exp_date = '3個月內' and p_level1 = '資訊商品類') "3個月內到期_資訊",
			count(distinct aa.member_id) filter (where exp_date = '6-9個月' and p_level1 = '資訊商品類') as "3-6個月到期_資訊",
			count(distinct aa.member_id) filter (where exp_date = '6-9個月' and p_level1 = '資訊商品類') as "6-9個月到期_資訊",
			count(distinct aa.member_id) filter (where exp_date = '9-12個月' and p_level1 = '資訊商品類') as "9-12個月到期_資訊",
			count(distinct aa.member_id) filter (where exp_date = '12個月以上' and p_level1 = '資訊商品類') as "12個月以上到期_資訊"
		from (select member_id, "%5$s" group_name_all from %4$s."%1$s" where "%5$s" is not null and "%5$s"!='新戶') aa
		left join pdata.mbr_profile_mart bb using (member_id)
		left join %4$s."%2$s" cc using (member_id)
		group by aa.group_name_all
		)
		select tmp.*
		from tmp
		left join (values ('小B',1), ('忠誠',2), ('高潛力',3), ('低潛力',4), ('待喚回忠誠',5), ('流失',6), 
						  ('沈睡1',7), ('沈睡2',8), ('沈睡3',9), ('沈睡4',10), ('沈睡5',11), ('沈睡6',12),
						  ('註冊未購1',13), ('註冊未購2',14), ('註冊未購3',15), ('註冊未購4',16), ('註冊未購5',17), 
						  ('註冊未購6',18), ('註冊未購7',19), 
					      ('其他',20)) t(t1,t2) on tmp.group_name_all = t.t1
		order by t.t2 asc
	);
	$f1$, _input_table_name1, _input_table_name2, output_table_name, _schema, first_date);
	execute _query1;
	
	raise notice '%','計算完成!!';
end;
$func$ language plpgsql;








-- 回購會員明細表
-- Input: 日期、輸出的表格名稱(預設schema為felixlin)
-- 不需先建立table，此function會drop已經存在的表，再重新建立一個新表
create or replace function felixlin.rfm_repo_detail(first_date text, output_table_name text)
returns void as $func$
declare 
	_schema text = 'felixlin';
	_query1 text;  
	_find text;
	_input_table_name1 text := $$Proj016_360_txn_rfm_buy_detail$$;
	_input_table_name2 text := $$Proj016_360_rfm_month$$;
begin
	-- query開始
	_query1 := 
	format(
	$f1$
	drop table if exists %4$s."%3$s";
	create table %4$s."%3$s" as
	(
		with tmp1 as  -- 同一商品
		(
			select 
				on_p_level1, member_id,	p_no,
				count(distinct order_date) cnt
			from %4$s."%1$s"
			where on_p_level1 is not null
				and on_p_level1 != ''
			group by on_p_level1, member_id, p_no
		), tmp2 as
		(
			select
				on_p_level1, member_id,
				count(distinct p_no) filter (where cnt > 1) as cnt1
			from tmp1
			group by on_p_level1, member_id
		), tmp3 as  -- 不同商品
		(
			select 
				on_p_level1, member_id,
				count(distinct p_no) cnt0
			from tmp1
			group by on_p_level1, member_id
		), tmp4 as -- 一次性購買
		(
			select 
				on_p_level1, member_id,
				count(distinct order_date) cnt2
			from %4$s."%1$s"
			where on_p_level1 is not null
				and on_p_level1 != ''
			group by on_p_level1, member_id
		)
		select 
			coalesce(tmp2.on_p_level1, tmp3.on_p_level1) as on_p_level1, 
			coalesce(tmp2.member_id, tmp3.member_id) as member_id, 
			cnt1 "同一商品",
			case 
				when cnt0 > 1 then 'Y'
				else 'N'
			end "不同商品",
			case  
				when cnt2 > 1 then 'N'
				else 'Y'
			end	"一次性購買",
			group_name_all
		from tmp2
		full outer join tmp3 using (on_p_level1, member_id)
		full outer join tmp4 using (on_p_level1, member_id)
		left join (select member_id, "%5$s" group_name_all from %4$s."%2$s" where "%5$s" is not null and "%5$s" != '新戶') using (member_id)
	);
	$f1$, _input_table_name1, _input_table_name2, output_table_name, _schema, first_date);
	execute _query1;
	
	raise notice '%','計算完成!!';
end;
$func$ language plpgsql;







-- 分眾各類別回購表
-- Input: 輸出的表格名稱(預設schema為felixlin)
-- 不需先建立table，此function會drop已經存在的表，再重新建立一個新表
create or replace function felixlin.rfm_repo_summary(first_date text, output_table_name text)
returns void as $func$
declare 
	_schema text = 'felixlin';
	_query1 text;  
	_find text;
	_input_table_name1 text := $$Proj016_360_txn_rfm_repo_detail$$;
begin
	-- query開始
	_query1 := 
	format(
	$f1$
	create table if not exists %3$s."%2$s"
	(
		"月份" date, on_p_level1 character varying,	group_name_all character varying, "會員數" integer,
		"同一商品回購" integer, "購買過不同商品" integer, "一次性購買" integer
	);$f1$, _input_table_name1, output_table_name, _schema, first_date);
	execute _query1;
	
	execute format($$select 1 from %3$s."%2$s" where "月份" = '%4$s' limit 1$$, _input_table_name1, output_table_name, _schema, first_date) into _find;
	if _find is not null then 
		raise notice '%','所選月份已經在TABLE裡了!!';
		return; 
	end if;
	
	_query1 := 
	format(
	$f2$
	insert into %3$s."%2$s"
	(
		with tmp as 
		(
			select 
				'%4$s'::date "月份",
				on_p_level1, group_name_all,
				count(distinct member_id) "會員數",
				count(distinct member_id) filter (where "同一商品" > 0) "同一商品回購",
				count(distinct member_id) filter (where "不同商品" = 'Y') "購買過不同商品",
				count(distinct member_id) filter (where "一次性購買" = 'Y') "一次性購買"
			from %3$s."%1$s"
			where group_name_all is not null
			group by on_p_level1, group_name_all
		)
		select 
			tmp.* 
		from tmp
		left join (values ('小B',1), ('忠誠',2), ('高潛力',3), ('低潛力',4), ('待喚回忠誠',5), ('流失',6), 
						  ('沈睡1',7), ('沈睡2',8), ('沈睡3',9), ('沈睡4',10), ('沈睡5',11), ('沈睡6',12),
						  ('註冊未購1',13), ('註冊未購2',14), ('註冊未購3',15), ('註冊未購4',16), ('註冊未購5',17), 
						  ('註冊未購6',18), ('註冊未購7',19), 
					      ('其他',20)) t(t1,t2) on tmp.group_name_all = t.t1
		--where not exists (select 1 from %3$s."%2$s" tt where tmp."月份" = '%4$s' and tmp.group_name_all = tt.group_name_all)
		order by on_p_level1, t.t2 asc
	);
	$f2$, _input_table_name1, output_table_name, _schema, first_date);
	execute _query1;
	
	raise notice '%','計算完成!!';
end;
$func$ language plpgsql;







-- 分眾APP點擊
-- Input: 計算日期(分眾時間)、輸出的表格名稱(預設schema為felixlin)
-- 不需先建立table，此function會drop已經存在的表，再重新建立一個新表
create or replace function felixlin.rfm_app_summary(first_date text, output_table_name text)
returns void as $func$
declare 
	_schema text = 'felixlin';
	_query1 text;  
	_find text;
	_input_table_name1 text := $$Proj016_360_rfm_month$$;
	_first_date text := date(date(first_date) - interval '1' month)::text;-------------------------
begin
	-- query開始
	_query1 := 
	format(
	$f1$
	create table if not exists %3$s."%2$s"
	(
		"月份" date, group_name_all text, "發送會員數" integer, "開信會員數" integer, "開信會員佔比" float
	);$f1$, _input_table_name1, output_table_name, _schema, first_date, _first_date);
	execute _query1;
	
	execute format($$select 1 from %3$s."%2$s" where "月份" = '%5$s' limit 1$$, _input_table_name1, output_table_name, _schema, first_date, _first_date) into _find;
	if _find is not null then 
		raise notice '%','所選月份已經在TABLE裡了!!';
		return; 
	end if;
	
	_query1 := 
	format(
	$f2$
	with tmp0 as   -- 分眾名單
	(
		select 
			member_id, 
			"%5$s" group_name_all 
		from %3$s."%1$s"
		where "%5$s" is not null 
			and "%5$s" != '新戶'
	), tmp1 as   -- 發送名單
	(
		select
			distinct register_id as member_id
		from edwadmin.senao_pushmessage_publish 
		where sent_date >= '%4$s'::date - interval '1' month
			and sent_date < '%4$s'
			and register_id is not null
			and register_id != ''
			and msg_id in (select msg_id from pdata.com_app_summary)
	), tmp2 as   -- 發送會員數
	(
		select 
			group_name_all,
			count(distinct tmp1.member_id) "發送會員數"
		from tmp1
		inner join tmp0 using (member_id)
		group by group_name_all
	), tmp3 as   -- 開信會員數
	(
		select 
			group_name_all,
			count(distinct aa.member_id) "開信會員數" 
		from ptemp.com_app_source aa
		inner join tmp0 using (member_id)
		where sent_date >= '%4$s'::date - interval '1' month----
			and sent_date < '%4$s'---
		group by group_name_all
	), tmp4 as   -- 合併
	(
		select 
			'%5$s'::date "月份",
			group_name_all,
			"發送會員數",
			"開信會員數",
			round("開信會員數"*1.0/"發送會員數", 3) as "開信會員佔比"
		from tmp2
		left join tmp3 using (group_name_all)
				left join (values ('小B',1), ('忠誠',2), ('高潛力',3), ('低潛力',4), ('待喚回忠誠',5), ('流失',6), 
						  ('沈睡1',7), ('沈睡2',8), ('沈睡3',9), ('沈睡4',10), ('沈睡5',11), ('沈睡6',12),
						  ('註冊未購1',13), ('註冊未購2',14), ('註冊未購3',15), ('註冊未購4',16), ('註冊未購5',17), 
						  ('註冊未購6',18), ('註冊未購7',19), 
						  ('其他',20)) t(t1,t2) on tmp2.group_name_all = t.t1
		order by t.t2 asc
	)
	insert into %3$s."%2$s"
	(
		select *
		from tmp4
		where not exists (select 1 from %3$s."%2$s" tt where tt."月份" = '%5$s' and tmp4.group_name_all = tt.group_name_all)
	);
	$f2$, _input_table_name1, output_table_name, _schema, first_date, _first_date);
	execute _query1;
	
	raise notice '%','計算完成!!';
end;
$func$ language plpgsql;






-- 分眾eDM點擊
-- Input: 輸出的表格名稱(預設schema為felixlin)
-- 不需先建立table，此function會drop已經存在的表，再重新建立一個新表
create or replace function felixlin.rfm_edm_summary(first_date text, output_table_name text)
returns void as $func$
declare 
	_schema text = 'felixlin';
	_query1 text;  
	_find text;
	_input_table_name1 text := $$Proj016_360_rfm_month$$;
	_first_date text := date(date(first_date) - interval '1' month)::text;--------------
begin
	-- query開始
	_query1 := 
	format(
	$f1$
	create table if not exists %3$s."%2$s"
	(
		"月份" date, group_name_all text, "發送會員數" integer, "開信會員數" integer, "開信會員佔比" float
	);$f1$, _input_table_name1, output_table_name, _schema, first_date, _first_date);
	execute _query1;
	
	execute format($$select 1 from %3$s."%2$s" where "月份" = '%5$s' limit 1$$, _input_table_name1, output_table_name, _schema, first_date, _first_date) into _find;
	if _find is not null then 
		raise notice '%','所選月份已經在TABLE裡了!!';
		return; 
	end if;
	
	_query1 := 
	format(
	$f2$
	with tmp0 as
	(
		select 
			member_id, 
			"%5$s" group_name_all 
		from %3$s."%1$s"
		where "%5$s" is not null 
			and "%5$s" != '新戶'
	), tmp1 as  -- 發送會員數
	(
		select
			distinct externrefnum as member_id
		from edwadmin.emailsendsuccesspool  -- 開信table
		where submittime >= '%4$s'::date - interval '1' month		--  發信時間起始
			and submittime < '%4$s'::date -- 發信時間截止日
			and externrefnum!=''
			and externrefnum is not null
			and subject is not null		-- 信件主旨不為null
			and subject not similar to '%%(中獎|測試)%%'
	), tmp3 as   -- 發送會員
	(
		select 
			group_name_all,
			count(distinct tmp1.member_id) "發送會員數" 
		from tmp1
		inner join tmp0 using (member_id)
		group by group_name_all
	), tmp4 as
	(
		select
			group_name_all,
			count(distinct tmp0.member_id) "開信會員數"
		from edwadmin.emailopenpool aa -- 開信table
		right join edwadmin.emailjobpool b using (launchid)
		inner join tmp0 on aa.externrefnum = tmp0.member_id
		where aa.submittime >= '%4$s'::date - interval '1' month	-- 發信時間起始
			and aa.submittime < '%4$s'::date	--  發信時間起始
			and aa.externrefnum!=''
			and aa.externrefnum is not null
			and subject is not null		-- 信件主旨不為null
			and subject not similar to '%%(中獎|測試)%%'
		group by group_name_all
	), tmp5 as
	(
		select 
			'%5$s'::date "月份",
			group_name_all,
			"發送會員數",
			"開信會員數",
			round("開信會員數"*1.0/"發送會員數", 3) as "開信會員佔比"
		from tmp3
		left join tmp4 using (group_name_all)
		left join (values ('小B',1), ('忠誠',2), ('高潛力',3), ('低潛力',4), ('待喚回忠誠',5), ('流失',6), 
						  ('沈睡1',7), ('沈睡2',8), ('沈睡3',9), ('沈睡4',10), ('沈睡5',11), ('沈睡6',12),
						  ('註冊未購1',13), ('註冊未購2',14), ('註冊未購3',15), ('註冊未購4',16), ('註冊未購5',17), 
						  ('註冊未購6',18), ('註冊未購7',19), 
						  ('其他',20)) t(t1,t2) on tmp3.group_name_all = t.t1
		order by t.t2 asc
	)
	insert into %3$s."%2$s"
	(
		select *
		from tmp5
		where not exists (select 1 from %3$s."%2$s" tt where tt."月份" = '%5$s' and tmp5.group_name_all = tt.group_name_all)
	);
	$f2$, _input_table_name1, output_table_name, _schema, first_date, _first_date);
	execute _query1;
	
	raise notice '%','計算完成!!';
end;
$func$ language plpgsql;







-- 喚醒的分眾人數
-- Input: 輸出的表格名稱(預設schema為felixlin)
-- 不需先建立table，此function會create/truncate
create or replace function felixlin.rfm_wake_summary1(first_date text, output_table_name text)
returns void as $func$
declare 
	_schema text = 'felixlin';
	_query1 text;  
	_find text;
	_input_table_name1 text := $$Proj016_360_rfm_month$$;
	_input_table_name2 text := $$Proj016_360_txn_rfm_buy_detail$$;
begin
	-- query開始
	first_date := date(first_date::date - interval '1' month)::text;
	_query1 := 
	format(
	$f1$
	create table if not exists %3$s."%2$s"
	(
		"月份" date, "分眾" text, "館別" text, "人數" integer
	);$f1$, _input_table_name1, output_table_name, _schema, first_date, _input_table_name2);
	execute _query1;
	
	execute format($$select 1 from %3$s."%2$s" where "月份" = '%4$s' limit 1$$, _input_table_name1, output_table_name, _schema, first_date, _input_table_name2) into _find;
	if _find is not null then 
		raise notice '%','所選月份已經在TABLE裡了!!';
		return; 
	end if;
	
	_query1 := 
	format(
	$f2$
	with tmp1 as 
	(
		select 
			member_id,
			case 
				when "%4$s" like '沈睡%%' then '沈睡'
				when "%4$s" like '註冊未購%%' then '註冊未購'
				else "%4$s"
			end group_name_all
		from felixlin."Proj016_360_rfm_month"
		where "%4$s" is not null 
			and "%4$s" != '新戶'
			and "%4$s" != '其他'
	), tmp2 as
	(
		select 
			tmp1.group_name_all "分眾", 
			on_p_level1 "館別",
			count(distinct member_id) "人數"
		from %3$s."%5$s" aa
		inner join tmp1 using (member_id)
		where order_date_m = '%4$s'
			and on_p_level1 != ''
		group by tmp1.group_name_all, on_p_level1
	)
	insert into %3$s."%2$s"
	(
		select 
			'%4$s'::date "月份",
			tmp2.*
		from tmp2 
		left join (values ('小B',1), ('忠誠',2), ('高潛力',3), ('低潛力',4), ('待喚回忠誠',5), ('流失',6), 
						  ('沈睡',7), ('註冊未購',11)) t(t1,t2) on tmp2."分眾" = t.t1
		order by t.t2 asc, "人數" desc
	);
	$f2$, _input_table_name1, output_table_name, _schema, first_date, _input_table_name2);
	execute _query1;
	
	raise notice '%','計算完成!!';
end;
$func$ language plpgsql;







-- 喚醒的購買館別
-- Input: 輸出的表格名稱(預設schema為felixlin)
-- 不需先建立table，此function會create/truncate
create or replace function felixlin.rfm_wake_summary2(first_date text, output_table_name text)
returns void as $func$
declare 
	_schema text = 'felixlin';
	_query1 text;  
	_find text;
	_input_table_name1 text := $$Proj016_360_rfm_month$$;
	_input_table_name2 text := $$Proj016_360_txn_rfm_buy_detail$$;
begin
	-- query開始
	first_date := date(first_date::date - interval '1' month)::text;
	_query1 := 
	format(
	$f1$
	create table if not exists %3$s."%2$s"
	(
		"月份" date, "分眾" text, "人數" integer, "喚回人數" integer, "喚回率" float
	);$f1$, _input_table_name1, output_table_name, _schema, first_date, _input_table_name2);
	execute _query1;
	
	execute format($$select 1 from %3$s."%2$s" where "月份" = '%4$s' limit 1$$, _input_table_name1, output_table_name, _schema, first_date, _input_table_name2) into _find;
	if _find is not null then 
		raise notice '%','所選月份已經在TABLE裡了!!';
		return; 
	end if;
	
	_query1 := 
	format(
	$f2$
	with tmp1 as
	(
		select member_id
		from %3$s."%5$s" 
		where order_date_m = '%4$s'
			and on_p_level1 != ''
	), tmp2 as
	(
		select 
			"%4$s" "分眾",
			count(distinct member_id) "人數",
			count(distinct member_id) filter (where member_id in (select member_id from tmp1)) "喚回人數"
		from %3$s."%1$s"
		where "%4$s" is not null 
			and "%4$s" != '新戶'
			and "%4$s" != '其他'
		group by "%4$s"
	)
	insert into %3$s."%2$s"
	(
		select 
			'%4$s'::date "月份",
			tmp2.*,
			round(tmp2."喚回人數"/(tmp2."人數"*1.0), 4)--------------------------------
		from tmp2
		left join (values ('小B',1), ('忠誠',2), ('高潛力',3), ('低潛力',4), ('待喚回忠誠',5), ('流失',6), 
						  ('沈睡1',7), ('沈睡2',8), ('沈睡3',9), ('沈睡4',10), ('沈睡5',11), ('沈睡6',12),
						  ('註冊未購1',13), ('註冊未購2',14), ('註冊未購3',15), ('註冊未購4',16), ('註冊未購5',17), 
						  ('註冊未購6',18), ('註冊未購7',19), 
						  ('其他',20)) t(t1,t2) on tmp2."分眾" = t.t1
		order by t.t2 asc
	)
	$f2$, _input_table_name1, output_table_name, _schema, first_date, _input_table_name2);
	execute _query1;
	
	raise notice '%','計算完成!!';
end;
$func$ language plpgsql;





create or replace function felixlin.rfm_segment_stats(first_date text, output_table_name text)
returns void as $func$
declare 
	_schema text = 'felixlin';
	_query1 text;  
	_find text;
	_input_table_name1 text := $$Proj016_360_rfm_month$$;
begin
	-- query開始
	_query1 := 
	format(
	$f1$
	create table if not exists %3$s."%2$s"
	(
		"月份" date, "VIP" integer, "S1" integer, "S2" integer, "A1" integer, 
		"A2" integer, "沈睡1" integer, "沈睡2" integer, "沈睡3" integer, 
		"沈睡4" integer, "沈睡5" integer, "沈睡6" integer, "註冊未購1" integer, 
		"註冊未購2" integer, "註冊未購3" integer, "註冊未購4" integer, 
		"註冊未購5" integer, "註冊未購6" integer, "註冊未購7" integer, "其他" integer,
		"會員數" integer, "新戶" integer
	);
	$f1$, _input_table_name1, output_table_name, _schema, first_date);
	execute _query1;
	
	execute format($$select 1 from information_schema.columns where table_schema = '%3$s' and table_name = '%1$s'
						and column_name = '%4$s'$$, _input_table_name1, output_table_name, _schema, first_date) into _find;
	if _find is null then 
		raise notice '%','所選月份不在自建分眾TABLE裡';
		return; 
	end if;
	
	execute format($$select 1 from %3$s."%2$s" where "月份" = '%4$s' limit 1$$, _input_table_name1, output_table_name, _schema, first_date) into _find;
	if _find is not null then 
		raise notice '%','所選月份已經在TABLE裡了!!';
		return; 
	end if;
	
	_query1 := 
	format(
	$f2$
	insert into %3$s."%2$s"
	(
		select 
			'%4$s'::date "月份",
			count(distinct member_id) filter (where "%4$s" = '忠誠') "VIP",
			count(distinct member_id) filter (where "%4$s" = '高潛力') "S1",
			count(distinct member_id) filter (where "%4$s" = '低潛力') "S2",
			count(distinct member_id) filter (where "%4$s" = '待喚回忠誠') "A1",
			count(distinct member_id) filter (where "%4$s" = '流失') "A2",
			count(distinct member_id) filter (where "%4$s" = '沈睡1') "沈睡1",
			count(distinct member_id) filter (where "%4$s" = '沈睡2') "沈睡2",
			count(distinct member_id) filter (where "%4$s" = '沈睡3') "沈睡3",
			count(distinct member_id) filter (where "%4$s" = '沈睡4') "沈睡4",
			count(distinct member_id) filter (where "%4$s" = '沈睡5') "沈睡5",
			count(distinct member_id) filter (where "%4$s" = '沈睡6') "沈睡6",
			count(distinct member_id) filter (where "%4$s" = '註冊未購1') "註冊未購1", 
			count(distinct member_id) filter (where "%4$s" = '註冊未購2') "註冊未購2",
			count(distinct member_id) filter (where "%4$s" = '註冊未購3') "註冊未購3",
			count(distinct member_id) filter (where "%4$s" = '註冊未購4') "註冊未購4", 
			count(distinct member_id) filter (where "%4$s" = '註冊未購5') "註冊未購5",
			count(distinct member_id) filter (where "%4$s" = '註冊未購6') "註冊未購6",
			count(distinct member_id) filter (where "%4$s" = '註冊未購7') "註冊未購7",
			count(distinct member_id) filter (where "%4$s" = '其他') "其他",
			count(distinct member_id) "會員數",
			count(distinct member_id) filter (where "%4$s" = '新戶') "新戶"
		from %3$s."%1$s"
		where "%4$s" is not null
	)
	$f2$, _input_table_name1, output_table_name, _schema, first_date);
	execute _query1;
	
	raise notice '%','計算完成!!';
end;
$func$ language plpgsql;



