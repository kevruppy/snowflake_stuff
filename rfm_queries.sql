UNSET START_DATE;
SET START_DATE = '2021-12-31'::DATE;

UNSET SEED;
SET SEED = 42;

CREATE OR REPLACE TEMPORARY TABLE BASE AS
(
SELECT
	$START_DATE - UNIFORM(1, 90, RANDOM($SEED)) AS DATE
FROM
	TABLE(GENERATOR(ROWCOUNT => 100000))
);

UNSET PERIOD_END_DATE;
SET PERIOD_END_DATE = (SELECT MAX(DATE) FROM BASE);

CREATE OR REPLACE TEMP TABLE TRANSACTIONS AS
SELECT
	 DATE
	,SHA2(ROUND(NORMAL(10000, 100, RANDOM($SEED)))) AS ACCOUNT_NO
	,ROUND(ABS(NORMAL(1000, 250, RANDOM($SEED)))) AS REV
FROM
	BASE SAMPLE(100000 ROWS);

CREATE OR REPLACE TEMP TABLE PREP AS
SELECT
     ACCOUNT_NO
    ,DATEDIFF(DAY, MAX(DATE), $PERIOD_END_DATE) AS R
    ,COUNT(*) AS F
    ,SUM(REV) AS M
    -- R_QUARTILES
    ,PERCENTILE_CONT(0.25) WITHIN GROUP(ORDER BY R) OVER() AS R_P25
    ,PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY R) OVER() AS R_P50
    ,PERCENTILE_CONT(0.75) WITHIN GROUP(ORDER BY R) OVER() AS R_P75
    -- F_QUARTILES
    ,PERCENTILE_CONT(0.25) WITHIN GROUP(ORDER BY F) OVER() AS F_P25
    ,PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY F) OVER() AS F_P50
    ,PERCENTILE_CONT(0.75) WITHIN GROUP(ORDER BY F) OVER() AS F_P75
    -- M_QUARTILES
    ,PERCENTILE_CONT(0.25) WITHIN GROUP(ORDER BY M) OVER() AS M_P25
    ,PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY M) OVER() AS M_P50
    ,PERCENTILE_CONT(0.75) WITHIN GROUP(ORDER BY M) OVER() AS M_P75
    -- R_CLUSTER
    ,CASE WHEN R < R_P25 THEN '1' WHEN R >= R_P25 AND R < R_P50 THEN '2' WHEN R >= R_P50 AND R < R_P75 THEN '3' ELSE '4' END AS R_CLUSTER
    -- F_CLUSTER
    ,CASE WHEN F < F_P25 THEN '1' WHEN F >= F_P25 AND F < F_P50 THEN '2' WHEN F >= F_P50 AND F < F_P75 THEN '3' ELSE '4' END AS F_CLUSTER
    -- M_CLUSTER
    ,CASE WHEN M < M_P25 THEN '1' WHEN M >= M_P25 AND M < M_P50 THEN '2' WHEN M >= M_P50 AND M < M_P75 THEN '3' ELSE '4' END AS M_CLUSTER
FROM
    TRANSACTIONS
GROUP BY
    ACCOUNT_NO;

SELECT
     R_CLUSTER
    ,F_CLUSTER
    ,M_CLUSTER
    ,COUNT(*)
FROM
    PREP
GROUP BY
     R_CLUSTER
    ,F_CLUSTER
    ,M_CLUSTER
ORDER BY
     R_CLUSTER
    ,F_CLUSTER
    ,M_CLUSTER;
