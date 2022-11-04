CREATE_SECOND_SETTLEMENT_TABLE = """
CREATE TABLE IF NOT EXISTS second_settlement (
날짜 VARCHAR(8),
시간 VARCHAR(6),
종목코드 VARCHAR(255),
시가 INT,
고가 INT,
저가 INT,
종가 INT,
등락율 FLOAT(4),
누적거래대금 INT,
체결강도 FLOAT(4),
거래대금증감 INT,
거래회전율 FLOAT(4),
전일거래량대비 FLOAT(4),
전일동시간거래량비율 FLOAT(4),
시가총액 FLOAT(4),
체결거래량 INT
)"""

CALC_SECOND_CANDLE = """
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
SUM(체결거래량) AS 체결거래량
FROM (SELECT s.*, ROW_NUMBER() OVER() rn FROM settlement s) settlement_rn
GROUP BY 날짜, 시간, 종목코드
ORDER BY 종목코드 ASC, 시간 ASC 
"""