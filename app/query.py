CREATE_STOM_DB = """
CREATE TABLE "000020" (
"index" INTEGER,
"현재가" REAL, "시가" REAL, "고가" REAL, "저가" REAL, "등락율" REAL, "당일거래대금" REAL, "체결강도" REAL, "거래대금증감" REAL,
"전일비" REAL, "회전율" REAL, "전일동시간비" REAL, "시가총액" REAL, "라운드피겨위5호가이내" REAL,
"초당매수수량" REAL, "초당매도수량" REAL,
"VI해제시간" REAL, "VI가격" REAL, "VI호가단위" REAL, "초당거래대금" REAL, "고저평균대비등락율" REAL,
"매도총잔량" REAL, "매수총잔량" REAL,
"매도호가5" REAL, "매도호가4" REAL, "매도호가3" REAL, "매도호가2" REAL, "매도호가1" REAL, "매수호가1" REAL, "매수호가2" REAL, "매수호가3" REAL, "매수호가4" REAL, "매수호가5" REAL, 
"매도잔량5" REAL, "매도잔량4" REAL, "매도잔량3" REAL, "매도잔량2" REAL, "매도잔량1" REAL, "매수잔량1" REAL, "매수잔량2" REAL, "매수잔량3" REAL, "매수잔량4" REAL, "매수잔량5" REAL, 
"매도수5호가잔량합" REAL
)"""

CREATE_SECOND_SETTLEMENT_TABLE = """
CREATE TABLE IF NOT EXISTS second_settlement (
날짜 VARCHAR(8), 시간 VARCHAR(6), 종목코드 VARCHAR(255),
시가 INT, 고가 INT, 저가 INT, 종가 INT,
등락율 FLOAT(4),
누적거래대금 INT,
체결강도 FLOAT(4),
거래대금증감 INT,
거래회전율 FLOAT(4),
전일거래량대비 FLOAT(4),
전일동시간거래량비율 FLOAT(4),
시가총액 FLOAT(4),
체결거래량 INT,
틱개수 INT
)"""

CREATE_SECOND_BIDASK_TABLE = """
CREATE TABLE IF NOT EXISTS second_bidask (
날짜 VARCHAR(8), 시간 VARCHAR(6), 종목코드 VARCHAR(255),
매도호가1 INT, 매도호가2 INT, 매도호가3 INT, 매도호가4 INT, 매도호가5 INT, 매도호가6 INT, 매도호가7 INT, 매도호가8 INT, 매도호가9 INT, 매도호가10 INT,
매수호가1 INT, 매수호가2 INT, 매수호가3 INT, 매수호가4 INT, 매수호가5 INT, 매수호가6 INT, 매수호가7 INT, 매수호가8 INT, 매수호가9 INT, 매수호가10 INT,
매도수량1 INT, 매도수량2 INT, 매도수량3 INT, 매도수량4 INT, 매도수량5 INT, 매도수량6 INT, 매도수량7 INT, 매도수량8 INT, 매도수량9 INT, 매도수량10 INT,
매수수량1 INT, 매수수량2 INT, 매수수량3 INT, 매수수량4 INT, 매수수량5 INT, 매수수량6 INT, 매수수량7 INT, 매수수량8 INT, 매수수량9 INT, 매수수량10 INT,
매도호가총잔량 INT, 매수호가총잔량 INT, 순매수잔량 INT, 순매도잔량 INT, 매수호가총잔량직전대비 INT, 매도호가총잔량직전대비 INT,
매수비율 FLOAT(4), 매도비율 FLOAT(4),
틱개수 INT
)"""

CALC_SECOND_SETTLEMENT = """
SELECT 날짜, 시간, 종목코드, 
FIRST_VALUE(현재가) OVER(PARTITION BY 날짜, 시간, 종목코드 ORDER BY rn ASC) AS 시가,
FIRST_VALUE(현재가) OVER(PARTITION BY 날짜, 시간, 종목코드 ORDER BY rn DESC) AS 종가,
MAX(고가) AS 고가,
MIN(저가) AS 저가,
FIRST_VALUE(등락율) OVER(PARTITION BY 날짜, 시간, 종목코드 ORDER BY rn DESC) AS 등락율,
FIRST_VALUE(누적거래대금) OVER(PARTITION BY 날짜, 시간, 종목코드 ORDER BY rn DESC) AS 누적거래대금,
FIRST_VALUE(체결강도) OVER(PARTITION BY 날짜, 시간, 종목코드 ORDER BY rn DESC) AS 체결강도,
FIRST_VALUE(거래대금증감) OVER(PARTITION BY 날짜, 시간, 종목코드 ORDER BY rn DESC) AS 거래대금증감,
FIRST_VALUE(거래회전율) OVER(PARTITION BY 날짜, 시간, 종목코드 ORDER BY rn DESC) AS 거래회전율,
FIRST_VALUE(전일거래량대비) OVER(PARTITION BY 날짜, 시간, 종목코드 ORDER BY rn DESC) AS 전일거래량대비,
FIRST_VALUE(전일동시간거래량비율) OVER(PARTITION BY 날짜, 시간, 종목코드 ORDER BY rn DESC) AS 전일동시간거래량비율,
FIRST_VALUE(시가총액) OVER(PARTITION BY 날짜, 시간, 종목코드 ORDER BY rn DESC) AS 시가총액,
SUM(체결거래량) AS 체결거래량,
COUNT(*) AS 틱개수
FROM (SELECT s.*, ROW_NUMBER() OVER() rn FROM settlement s) settlement_rn
GROUP BY 날짜, 시간, 종목코드
ORDER BY 종목코드 ASC, 시간 ASC 
"""

CALC_SECOND_BIDASK = """
WITH bidask_numbered AS (
  SELECT b.*, ROW_NUMBER() OVER () AS rn
  FROM bidask AS b
), bidask_grouped_and_ranked AS (
  SELECT bn.*, 
  ROW_NUMBER() OVER (PARTITION BY 날짜, 시간, 종목코드 ORDER BY rn DESC) AS grouped_rn,
  ROW_NUMBER() OVER (PARTITION BY 날짜, 시간, 종목코드 ORDER BY rn ASC) AS record_count
  FROM bidask_numbered AS bn
), second_end_bidask AS (
  SELECT 날짜, 시간, 종목코드, record_count 틱개수,
  매도호가1, 매도호가2, 매도호가3, 매도호가4, 매도호가5, 매도호가6, 매도호가7, 매도호가8, 매도호가9, 매도호가10,
  매수호가1, 매수호가2, 매수호가3, 매수호가4, 매수호가5, 매수호가6, 매수호가7, 매수호가8, 매수호가9, 매수호가10,
  매도수량1, 매도수량2, 매도수량3, 매도수량4, 매도수량5, 매도수량6, 매도수량7, 매도수량8, 매도수량9, 매도수량10,
  매수수량1, 매수수량2, 매수수량3, 매수수량4, 매수수량5, 매수수량6, 매수수량7, 매수수량8, 매수수량9, 매수수량10,
  매도호가총잔량, 매수호가총잔량, 순매수잔량, 순매도잔량, 매수비율, 매도비율
  FROM bidask_grouped_and_ranked
  WHERE grouped_rn = 1
 )
 SELECT seb.*,
 LAG(매수호가총잔량) OVER (PARTITION BY 종목코드 ORDER BY 시간 ASC) - 매수호가총잔량 AS 매수호가총잔량직전대비,
 LAG(매도호가총잔량) OVER (PARTITION BY 종목코드 ORDER BY 시간 ASC) - 매도호가총잔량 AS 매도호가총잔량직전대비
 FROM second_end_bidask seb
 ORDER BY 종목코드, 시간
"""