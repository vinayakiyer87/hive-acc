SELECT
--TO_CHAR(SYSDATE,'DD-MM-YYYY HH24:MI:SS')SYS_DATE,
SUM(CASE WHEN SUBSTR(I025_POS_COND,1,2) IN ('08','59','81','01') THEN 1 ELSE 0 END) ONLINE_TXN_CNT,
round(((sum(CASE WHEN SUBSTR(I025_POS_COND,1,2) IN ('08','59','81','01') THEN i006_amt_bill end))/10000000),2) ONLINE_TXN_AMNT,
SUM(CASE WHEN I019_ACQ_COUNTRY NOT IN ('In','Ind','356') THEN 1 ELSE 0 END) INTERNATIONAL_TXN_CNT,
round(((sum(CASE WHEN I019_ACQ_COUNTRY NOT IN ('In','Ind','356') THEN i006_amt_bill END) )/10000000),2)INTERNATIONAL_TXN_AMNT,
round(((sum(i006_amt_bill))/10000000),2) total_trxn_amnt,
COUNT(1) COUNT_OF_TRXN,

UPPER(case when  mt.mergroup = 'HOTELS-V' then 'Hotels'
     when  mt.mergroup = 'CLOTHING S' then 'All Clothing Stores'
     when  mt.mergroup = 'SERVICE PR' then 'SERVICE PROVIDERS'
     when  mt.mergroup = 'DIY AND HO' then 'DIY and Household Stores'
     when  mt.mergroup = 'ENTERTAINM' then 'All Entertainment'
     when  mt.mergroup = 'AIRLINES-V' then 'Airlines'
     when  mt.mergroup = 'CAR RENT-V' then 'Car Rental Companies'
     when  mt.mergroup = 'RESTAURA-V' then 'Restaurants'
     when  mt.mergroup = 'OTHERS-V  ' then 'All other Merchants'
     when  MT.MERGROUP = 'MAIL ORD-V' then 'Mail Order / Telephone Order Providers'
     when mt.mergroup  = 'DIGI GOODS' then 'Digital Goods' else 'Others' end ) as MERCHANTGROUP
from online_db.AUTHORIZATIONS2 a,ods_hive.ctl_dropme_mertypes mt
where a.i018_merch_type=mt.code
and (a.I039_RSP_CD = 00 or a.I039_RSP_CD = 01)
group by (case when  mt.mergroup = 'HOTELS-V' then 'Hotels'
     when  mt.mergroup = 'CLOTHING S' then 'All Clothing Stores'
     when  mt.mergroup = 'SERVICE PR' then 'SERVICE PROVIDERS'
     when  mt.mergroup = 'DIY AND HO' then 'DIY and Household Stores'
     when  mt.mergroup = 'ENTERTAINM' then 'All Entertainment'
     when  mt.mergroup = 'AIRLINES-V' then 'Airlines'
     when  mt.mergroup = 'CAR RENT-V' then 'Car Rental Companies'
     when  mt.mergroup = 'RESTAURA-V' then 'Restaurants'
     when  mt.mergroup = 'OTHERS-V  ' then 'All other Merchants'
     WHEN  MT.MERGROUP = 'MAIL ORD-V' THEN 'Mail Order / Telephone Order Providers'
     when mt.mergroup  = 'DIGI GOODS' then 'Digital Goods' else 'Others' end );