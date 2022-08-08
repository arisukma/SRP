with Master AS
(select k.uwi ,k.well_name, k.bfpd1, k.bfpd2, k.bfpd3, k.bfpd_date1, k.bfpd_date2, k.bfpd_date3, k.BFPD_TEST_MIN, k.BFPD_TEST_MAX ,0 as Y1, -(k.bfpd_date2-k.bfpd_date1) as Y2, -(k.bfpd_date3-k.bfpd_date1) as Y3 from (
select t.uwi, t.well_name, 
SUM(DECODE(t.rnk,1,t.bfpd_test)) AS BFPD1, 
SUM(DECODE(t.rnk,2,t.bfpd_test)) AS BFPD2, 
SUM(DECODE(t.rnk,3,t.bfpd_test)) AS BFPD3,
MAX(DECODE(t.rnk,1,t.dyno_date)) AS bfpd_date1, 
MAX(DECODE(t.rnk,2,t.dyno_date)) AS bfpd_date2, 
MAX(DECODE(t.rnk,3,t.dyno_date)) AS bfpd_date3,
MAX(t.dyno_date) AS LATEST_TEST_DATE,
MIN(t.bfpd_test) AS BFPD_TEST_MIN,
MAX(t.bfpd_test) AS BFPD_TEST_MAX
from (select uwi, well_name, dyno_date, bfpd_test, RANK() OVER (PARTITION BY uwi ORDER BY dyno_date DESC) rnk 
        from IODSC_SOR.SRP_INFERRED_PRODUCTION where BFPD_TEST >=0 AND TRUNC(DYNO_DATE) < TRUNC(SYSDATE-3-1) order by uwi,dyno_date DESC) t 
group by t.uwi, t.well_name) k)
,
Master2 AS(
select UWI, REGR_SLOPE(Y,X) Slope, REGR_INTERCEPT(Y,X) Intercept from(
select t1.UWI, t1.WELL_NAME, t1.K, t2.L, t1.X, t2.Y from
(select uwi, well_name, K, X from master 
unpivot
(X for K in (bfpd1, bfpd2, bfpd3))
unpiv) t1 

LEFT JOIN  
(
select uwi, L, Y from master 
unpivot
(Y for L in (Y1, Y2, Y3))
unpiv) t2
ON t1.uwi = t2.uwi AND SUBSTR(t1.K, - 1) = SUBSTR(t2.L, - 1))
group by UWI)

select SYSDATE-3, master2.uwi, master2.SLOPE as BFPD_TEST_SLOPE, master2.Intercept as BFPD_TEST_INTERCEPT, master.BFPD1, master.BFPD2, master.BFPD3, 
master.BFPD_TEST_MIN, master.BFPD_TEST_MAX, master.bfpd_date1, master.bfpd_date2, master.bfpd_date3, 
case when master.bfpd_date3 IS NOT NULL then master.bfpd_date3
  when master.bfpd_date3 IS NULL and master.bfpd_date2 is NOT NULL then master.bfpd_date2
    else master.bfpd_date1 
      end as REG_REF_DATE,
case when master.bfpd_date3 IS NOT NULL then TRUNC(SYSDATE)-3-TRUNC(master.bfpd_date3)
  when master.bfpd_date3 IS NULL and master.bfpd_date2 is NOT NULL then TRUNC(SYSDATE)-3-TRUNC(master.bfpd_date2)
    else TRUNC(SYSDATE)-3-TRUNC(master.bfpd_date1)
      end as DAYS_AFTER_TEST,
 master.Y1, master.Y2, master.Y3 from Master2 left join Master on master.uwi = master2.uwi
