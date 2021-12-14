--query bảng Sales MTD - DailySales 
-- 12.04.2021: function nmv, gmv = selling_price *qty
-- 29.03.2021: combine order tracking
with T1 as
(
select 
id,
date(order_created_at) as orderdate,
status,
--fixname
case when UPPER(sku_name) like '%ST.IVES%' and upper(brand) = 'UI MASS' THEN 'ST.IVES' --brand cua UI MASS bị lỗi
		 when UPPER(sku_name) like '%DOVE%' and upper(brand) = 'UI MASS' THEN 'DOVE'--brand cua UI MASS bị lỗi
     else V_Brand_processing(brand, shop_account, platform) end as brand, -- function xử lý brand, details: public ==> functions ==> V_Brand_processing
CASE
		when upper(platform) like '%B2B%' then 'B2B'
		WHEN UPPER(platform) = 'ADAYROI' THEN 'ADR'
		WHEN UPPER(platform) = 'BOSCHDOTCOM' THEN 'BRAND.COM'
		else platform
end as platform,
quantity,
selling_price,
-- CASE --old 
-- WHEN status NOT IN ('Canceled','Returned','Lost','Failed') then
-- round((quantity*selling_price)/23000,0) else 0 end as nmv,
--CASE -- NMV,GMV
			--WHEN platform = 'LZD' AND payment_type = '' AND date(order_created_at) >= '2020-11-01' AND UPPER(order_type) = 'PRESALE' THEN 0 --05/11/2020: update CP presale LZD 11.11
			
			--WHEN upper(status) NOT IN ('CANCELED','RETURNED','LOST','FAILED', 'RETURNING')  AND platform = 'LZD' AND (paid_price < selling_price) THEN round((quantity*paid_price + platform_voucher)/23300,0) -- 12/2020 platform_voucher không ảnh hưởng đến NMV của Brand
			
			--WHEN upper(status)  NOT IN ('CANCELED','RETURNED','LOST','FAILED', 'RETURNING')  AND platform = 'LZD' AND (paid_price = selling_price) THEN round((quantity*paid_price)/23300,0)
			-- 01/12/2020: update NMV Lazada do platform voucher khong giam doanh thu
			
			--sàn tiki có gift có giá niêm yết - selling price ở database
			--WHEN upper(status) NOT IN ('CANCELED','RETURNED','LOST','FAILED', 'RETURNING')  AND UPPER(platform) = 'TIKI' and date(order_created_at) >= '2021-01-01'and date(order_created_at) <= '2021-03-31' AND upper(sku_name) like '%[GIFT]%' THEN 0 --một số gift TIKI có giá do listing 
			--WHEN upper(status)  NOT IN ('CANCELED','RETURNED','LOST','FAILED', 'RETURNING')  AND UPPER(platform) = 'TIKI' and date(order_created_at) >= '2021-01-01' and date(order_created_at) <= '2021-03-31' AND upper(sku) like '%FOC%' THEN 0 --một số gift TIKI có giá do listing 
		
			--WHEN upper(status)  NOT IN ('CANCELED','RETURNED','LOST','FAILED', 'RETURNING')  AND UPPER(platform) = 'TIKI' and date(order_created_at) >= '2021-04-01' AND income < 0 THEN 0 
		
			--WHEN upper(status)  NOT IN ('CANCELED','RETURNED','LOST','FAILED', 'RETURNING') THEN round((quantity*selling_price)/23300,0)
			--ELSE 0 END AS nmv, 
	
--new NMV 12/04/2021	
v_nmv_calculation(payment_type , platform , order_created_at , status , order_type , selling_price , paid_price , sku , sku_name,platform_voucher , quantity , income , platform_fee , subsidy , seller_voucher , order_value,original_price) as nmv, --function NMV		
			
--CASE

--WHEN platform = 'LZD' AND (paid_price < selling_price) THEN round((quantity*(paid_price + platform_voucher))/23300,0) -- 01/12/2020: update GMV Lazada
--WHEN platform = 'LZD' AND (paid_price = selling_price) THEN round((quantity*paid_price)/23300,0) -- 01/12/2020: update GMV Lazada
--WHEN UPPER(platform) = 'TIKI' and date(order_created_at) >= '2021-01-01' AND upper(sku_name) like '%[GIFT]%' THEN 0 --một số gift TIKI có giá 
--WHEN UPPER(platform) = 'TIKI' and date(order_created_at) >= '2021-01-01' AND upper(sku) like '%FOC%' THEN 0 -- một số gift TIKI có giá 
--When UPPER(platform) = 'TIKI' and date(order_created_at) >= '2021-01-01' AND income < 0 then 0
--ELSE 
round((quantity*selling_price)/23300,0) as gmv
FROM main_order2019
WHERE date(order_created_at) >= '2020-01-01'
--date(order_created_at) >= date(date_trunc('month', now()- interval '11 month'))
and upper(platform) not in ('ONPOINT')-- chi lay so cua thang nay
ORDER BY date(order_created_at) DESC, brand ASC
)
,T1A as
(
SELECT 
orderdate,
upper(brand) as brand,
upper(platform) as platform,
count(DISTINCT id) as ordernumber,
sum(quantity) as itemnumber,
case when cast (orderdate as date) <= date(now() - interval '1 day') then 1 else 0 end as ytd,
sum(nmv) as nmv,
sum(gmv) as gmv
from T1
group by
1,2,3
)

,T2 as
(
select
cast (date as date) as cal_date,
case when cast (date as date) <= date(now()) then 1 else 0 end as todayyn,
case when cast (date as date) <= date(now() - interval '1 day') then 1 else 0 end as ytdyn,
upper(brand) as cal_brand,
case when
upper(platform) like '%B2B%' then 'B2B' else upper(platform) end as cal_platform,
day_type as cal_daytype2,
case 
when upper(platform) like '%B2B%' or day_type = 'B2B' then 'B2B' 
when day_type = 'BL' then 'BL'
when day_type in ('CP','MG','SMG') then 'CP' else 'BL' end as cal_daytype,

cast (order_target as int) as cal_ordertarget,
cast (nmv_target as int) as cal_nmvtarget
from calendar 
where cast (date as date) >= '2020-01-01'
--date(date_trunc('month', now() - interval '11 month' ))
)
		
		-- 29/10: Bảng mới để group lại các target:
,T2_1 AS
			(
			SELECT 
				cal_date,
				todayyn,
				ytdyn,
				cal_brand,
				cal_platform,
				cal_daytype2,
				cal_daytype,
				SUM(cal_ordertarget) AS cal_ordertarget,
				SUM(cal_nmvtarget) AS cal_nmvtarget
		FROM T2
		GROUP BY 1,2,3,4,5,6,7
		)
,T2A as
(
select 
*,
case when cal_daytype = 'BL' then cal_nmvtarget else 0 end as cal_blnmvtarget,
case when cal_daytype = 'B2B' then cal_nmvtarget else 0 end as cal_b2bnmvtarget,
case when cal_daytype = 'CP' then cal_nmvtarget else 0 end as cal_cpnmvtarget,
cal_nmvtarget*todayyn as cal_mtdnmvtarget,
case when cal_daytype = 'BL' then cal_nmvtarget*todayyn else 0 end as cal_blmtdnmvtarget, --lay so cua ngay hom qua
case when cal_daytype = 'B2B' then cal_nmvtarget*todayyn else 0 end as cal_b2bmtdnmvtarget,
case when cal_daytype = 'CP' then cal_nmvtarget*todayyn else 0 end as cal_cpmtdnmvtarget
from T2_1
)
, T3 AS
(
select 
COALESCE(orderdate,cal_date) as orderdate,
COALESCE(ytd,0) as ytd,
COALESCE(brand,cal_brand) as brand,
COALESCE(platform,cal_platform) as platform,
COALESCE(ordernumber,0) as ordernumber,
COALESCE(itemnumber,0) as itemnumber,
COALESCE(nmv,0) as nmv,
COALESCE(gmv,0) as gmv,
COALESCE(cal_daytype,'NOTARGET') as cal_daytype,
COALESCE(cal_daytype2,'NOTARGET') as cal_daytype2,
cal_ordertarget,
cal_nmvtarget,
cal_blnmvtarget,
cal_b2bnmvtarget,
cal_cpnmvtarget,
cal_mtdnmvtarget,
cal_blmtdnmvtarget,
cal_b2bmtdnmvtarget,
cal_cpmtdnmvtarget
from T1A
FULL JOIN T2A
On T1A.orderdate = T2A.cal_date
AND T1A.brand = T2A.cal_brand
AND T1A.platform = T2A.cal_platform
)
, T3B AS
(
select
orderdate,
EXTRACT(MONTH from orderdate) as month,
case when cast (orderdate as date) <= date(now())then 1 else 0 end as today,
brand,
platform,
case --platformshare
	when UPPER(platform) not in ('LZD','TIKI','SENDO','SHOPEE','B2B') then 'OTHER'
	else platform
	end as platformshare,
case --model
when UPPER(brand) in ('EVASHOES','WATSONS','ESTEE LAUNDER') or (UPPER(brand) != 'BOSCHPT' and upper(platform)='BRAND.COM') or (upper(platform)='CRM')then 'SERVICE' else 'DISTRIBUTION' 
end as model,
--category
V_category_processing(brand) as category,--function mapping category
--groupbrand
V_Group_processing(brand,platform) as groupbrand, --function mapping groupbrand
ordernumber,
itemnumber,
nmv,
gmv,
case when cal_daytype = 'BL' then nmv else 0 end as blnmv, --lay so hom qua
case when cal_daytype = 'B2B' then nmv else 0 end as b2bnmv,
case when cal_daytype = 'CP' then nmv else 0 end as cpnmv,
case when cal_daytype = 'NOTARGET' then nmv else 0 end as othernmv,
cal_daytype,
cal_daytype2,
cal_ordertarget,
cal_nmvtarget,
cal_blnmvtarget,
cal_b2bnmvtarget,
cal_cpnmvtarget,
cal_mtdnmvtarget,
cal_blmtdnmvtarget,
cal_b2bmtdnmvtarget,
cal_cpmtdnmvtarget
From T3
)

,T4 as (select 
*,
V_director_processing(groupbrand,orderdate) as group_director, --functions director 03/02/2021
case
		when upper(groupbrand) in ('LVN ACD'--update01/02/2021
															
															 )
															 then 'Nhung Do'
										 
		when upper(groupbrand) in ('UNICHARM',--update12/10
													'SAMSUNG', 'BROTHER', 'BOSCH'
													)then 'Duy Anh'

		when upper(groupbrand) in --update12/10
															('LVN CPD',
															 'KC',
															 'MILAGANICS',
															 'FONTERRA',
															 'AVENE',
															 'BOBINI',
															 'KARMART')
															 then 'Hong Nhung'
															 
						when upper(groupbrand) in --update18/11
															('NIVEA',
															'P&G',
															'TAISUN','LG')
															 then 'Linh Thai'
						when upper(groupbrand) in --update23/11/2020
															('SHISEIDO CPC',
															 'SHISEIDO',
															 'SHISEIDO PREMIUM',															 
															 'UI MASS') then 'Thuy Nguyen'	
				
		
						when orderdate >= '2021-03-01' and upper(groupbrand) in ('LVN LUXE') then 'Bang Tam'
						when orderdate >= '2021-03-01' and upper(groupbrand) in ('PPD', 'CJ INNERB', 'MONDE POINT', 'HAFELE') then 'Yen Mai'
						
	
			 else 'OTHERS'
			 end as GBM
from T3B
WHERE UPPER(brand) <> 'MOIRA' )

,O1 as (
select 
id,
date(order_created_at) as orderdate,
status,

--fixname
case when UPPER(sku_name) like '%ST.IVES%' and upper(brand) = 'UI MASS' THEN 'ST.IVES' --brand cua UI MASS bị lỗi
		 when UPPER(sku_name) like '%DOVE%' and upper(brand) = 'UI MASS' THEN 'DOVE'
			else V_Brand_processing(brand, shop_account, platform) end as brand,
CASE
		WHEN UPPER(platform) like '%B2B%' THEN 'B2B'
		WHEN UPPER(platform) = 'ADAYROI' THEN 'ADR'
		WHEN UPPER(platform) = 'BOSCHDOTCOM' THEN 'BRAND.COM'
		else platform
end as platform,
quantity
FROM main_order2019
WHERE date(order_created_at) >= '2020-01-01' AND UPPER(brand) <> 'MOIRA' AND UPPER(platform) <> 'ONPOINT' AND upper(status) not in ('CANCELED','RETURNED','LOST','FAILED', 'RETURNING')  )

, O2 AS (
SELECT *,
v_group_processing(brand,platform) as groupbrand,
case --platformshare
	when UPPER(platform) not in ('LZD','TIKI','SENDO','SHOPEE','B2B') then 'OTHER'
	else platform
	end as platformshare
	from O1 )
	
	, O3 as (
	SELECT orderdate, brand, groupbrand, platformshare,platform,
				count (distinct id) as net_order1,
				sum(quantity) as net_item1
				from O2 group by 1,2,3,4,5 )
				
				select T4.*, O3.net_order1, O3.net_item1 from T4 left join O3 on T4.orderdate = O3.orderdate
				and T4.brand = O3.brand and T4.groupbrand = O3.groupbrand and T4.platformshare = O3.platformshare and T4.platform = O3.platform