UNSET START_DATE;
SET START_DATE = '2022-04-01'::DATE;

UNSET SEED;
SET SEED = 42;

CREATE OR REPLACE TEMPORARY TABLE BASE AS
(
SELECT
	$START_DATE - UNIFORM(1, 90, RANDOM($SEED)) AS DATE
FROM
	TABLE(GENERATOR(ROWCOUNT => 100000))
);

CREATE OR REPLACE TEMPORARY TABLE ORDERS AS
(
WITH PREP AS
(
SELECT
	 DATE
	,DECODE(UNIFORM(1, 6, RANDOM($SEED)),
			1, 'Coke',
			2, 'Soda',
			3, 'Beer',
			4, 'Chips',
			5, 'Gum',
			6, 'Ice-Cream') AS PRODUCT
	,CEIL(ABS(NORMAL(4, 2, RANDOM($SEED)))) AS ORDERS
FROM
	BASE SAMPLE(10000 ROWS)
)

SELECT
	 DATE
	,PRODUCT
	,SUM(ORDERS) AS ORDERS
FROM
	PREP
GROUP BY	
	 DATE
	,PRODUCT
);

SELECT
	 PRODUCT
	,SUM(ORDERS) AS ORDERS_PER_PRODUCT
	,ROUND(100 * RATIO_TO_REPORT(ORDERS_PER_PRODUCT) OVER(), 2) AS SHARE_OF_ORDERS
	,SUM(SUM(ORDERS)) OVER() AS TOTAL_ORDERS
FROM
	ORDERS
GROUP BY
	PRODUCT
ORDER BY
	ORDERS_PER_PRODUCT DESC;
	
SELECT
	  DATE
	 ,PRODUCT
	 ,SUM(ORDERS) AS ORDERS
	 ,SUM(SUM(ORDERS)) OVER(PARTITION BY PRODUCT ORDER BY DATE ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS CUM_ORDERS_PER_PRODUCT
	 ,SUM(SUM(ORDERS)) OVER(PARTITION BY PRODUCT, MONTH(DATE)) AS ORDERS_PER_PRODUCT_AND_MONTH
	 ,SUM(SUM(ORDERS)) OVER(PARTITION BY PRODUCT, MONTH(DATE) ORDER BY DATE ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS CUM_ORDERS_PER_PRODUCT_AND_MONTH
	 ,ROUND(CUM_ORDERS_PER_PRODUCT_AND_MONTH/ORDERS_PER_PRODUCT_AND_MONTH * 100, 2) AS CUM_PCT_ORDERS_PER_PRODUCT_AND_MONTH
	 ,ROUND(AVG(SUM(ORDERS)) OVER(PARTITION BY PRODUCT ORDER BY DATE ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS MOVG_AVG_7_DAYS_PER_PRODUCT
	 ,LAG(SUM(ORDERS)) OVER(PARTITION BY PRODUCT ORDER BY DATE) AS ORDERS_PRIOR_DAY_PER_PRODUCT
	 ,SUM(ORDERS) - LAG(SUM(ORDERS)) OVER(PARTITION BY PRODUCT ORDER BY DATE) AS ABS_DELTA_TO_YESTERDAY
	 ,ROUND(((SUM(ORDERS)/LAG(SUM(ORDERS)) OVER(PARTITION BY PRODUCT ORDER BY DATE)) -1) * 100, 2) AS PCT_DELTA_TO_YESTERDAY
FROM
	ORDERS
GROUP BY
	 DATE
	,PRODUCT
ORDER BY
	DATE;