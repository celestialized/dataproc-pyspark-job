SELECT  O.SOURCE_DC_NBR AS GDC,
            O.STORE_NBR,
            I.FINELINE_NBR,
            O.ITEM_NBR,
            AVG(DATE_DIFF(O.ARRIVE_DATE, O.SCHED_SHIP_DATE, DAY)) AS AVG_GDC_TO_STORE_LEADTIME,
            IEEE_DIVIDE(COUNT(DISTINCT O.ORDER_DATE),COUNT(DISTINCT W.WM_YR_WK_ID)) AS GDC_STORE_REPLENISHMENT_FREQUENCY,
            Count(O.ORDER_DATE) AS CNT_STR_ITEM_PO,
            SUM(O.ORDER_EACH_QTY) AS GDC_ITEM_QTY_IN_EACHES
   FROM wmt-edw-prod.US_WM_REPL_VM.GRS_ORDER O
   JOIN POS_ITEMS I ON O.SOURCE_DC_NBR= I.GDC
   AND O.STORE_NBR = I.STORE_NBR
   AND O.ITEM_NBR = I.ITEM_NBR
   JOIN (SELECT  WM_YR_WK_ID,CALENDAR_DATE
                FROM `wmt-edw-prod`.US_CORE_DIM_VM.CALENDAR_DIM
                WHERE CALENDAR_DATE >= DATE_SUB(CURRENT_DATE(), INTERVAL VWEEK WEEK)
                AND CALENDAR_DATE <= CURRENT_DATE())W ON W.CALENDAR_DATE = O.ORDER_DATE
   WHERE O.ARRIVE_DATE >= DATE_SUB(CURRENT_DATE(), INTERVAL VWEEK WEEK)
   AND O.ARRIVE_DATE <= CURRENT_DATE()
   GROUP BY O.SOURCE_DC_NBR,
            O.STORE_NBR,
            I.FINELINE_NBR,
            O.ITEM_NBR