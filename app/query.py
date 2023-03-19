CREATE_STOM_TABLE = """
CREATE TABLE IF NOT EXISTS '%s' (
"index" INTEGER,
"현재가" REAL, "시가" REAL, "고가" REAL, "저가" REAL, "등락율" REAL, "당일거래대금" REAL, "체결강도" REAL, "거래대금증감" REAL,
"전일비" REAL, "회전율" REAL, "전일동시간비" REAL, "시가총액" REAL, "라운드피겨위5호가이내" REAL,
"초당매수수량" REAL, "초당매도수량" REAL,
"VI해제시간" REAL, "VI가격" REAL, "VI호가단위" REAL, "초당거래대금" REAL, "고저평균대비등락율" REAL,
"매도총잔량" REAL, "매수총잔량" REAL,
"매도호가5" REAL, "매도호가4" REAL, "매도호가3" REAL, "매도호가2" REAL, "매도호가1" REAL, "매수호가1" REAL, "매수호가2" REAL, "매수호가3" REAL, "매수호가4" REAL, "매수호가5" REAL, 
"매도잔량5" REAL, "매도잔량4" REAL, "매도잔량3" REAL, "매도잔량2" REAL, "매도잔량1" REAL, "매수잔량1" REAL, "매수잔량2" REAL, "매수잔량3" REAL, "매수잔량4" REAL, "매수잔량5" REAL, 
"매도수5호가잔량합" REAL,
"초당체결틱수" INT, "초당호가틱수" INT
)"""

CREATE_SECOND_MERGED_TABLE = """
CREATE TABLE IF NOT EXISTS 'second_total' (
날짜 VARCHAR(8), 시간 VARCHAR(6), 종목코드 VARCHAR(255),
현재가 INTEGER, 시가 INTEGER, 고가 INTEGER, 저가 INTEGER,
등락율 FLOAT(4), 
누적거래대금 INT, 
체결강도 FLOAT(4), 거래대금증감 INT,
전일거래량대비 FLOAT(4), 거래회전율 FLOAT(4), 전일동시간거래량비율 FLOAT(4), 시가총액 FLOAT(4), 초당매수수량 INT, 초당매도수량 INT, 초당거래대금 FLOAT(4),
매도호가총잔량 INT, 매수호가총잔량 INT,
매도호가10 INT, 매도호가9 INT, 매도호가8 INT, 매도호가7 INT, 매도호가6 INT, 매도호가5 INT, 매도호가4 INT, 매도호가3 INT, 매도호가2 INT, 매도호가1 INT, 
매수호가1 INT, 매수호가2 INT, 매수호가3 INT, 매수호가4 INT, 매수호가5 INT, 매수호가6 INT, 매수호가7 INT, 매수호가8 INT, 매수호가9 INT, 매수호가10 INT,
매도수량10 INT, 매도수량9 INT, 매도수량8 INT, 매도수량7 INT, 매도수량6 INT, 매도수량5 INT, 매도수량4 INT, 매도수량3 INT, 매도수량2 INT, 매도수량1 INT, 
매수수량1 INT, 매수수량2 INT, 매수수량3 INT, 매수수량4 INT, 매수수량5 INT, 매수수량6 INT, 매수수량7 INT, 매수수량8 INT, 매수수량9 INT, 매수수량10 INT,
초당체결틱수 INT, 초당호가틱수 INT)
"""

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
초당매수수량 INT,
초당매도수량 INT,
초당매수거래대금 FLOAT(4),
초당매도거래대금 FLOAT(4),
초당거래대금 FLOAT(4),
틱개수 INT,
최종수신번호 INT
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
틱개수 INT,
최종수신번호 INT
)"""

CREATE_DETAILED_TICK_TABLE = """
CREATE TABLE IF NOT EXISTS detailed_tick (
날짜 VARCHAR(8), 시간 VARCHAR(6), 수신시간 VARCHAR(15), 수신번호 INT, 종목코드 VARCHAR(255),
현재가 INT, 시가 INT, 고가 INT, 저가 INT, 
등락율 FLOAT(4),
누적거래대금 INT,
체결강도 FLOAT(4),
거래대금증감 INT,
전일거래량대비 FLOAT(4),
거래회전율 FLOAT(4),
전일동시간거래량비율 FLOAT(4),
시가총액 FLOAT(4),
매수거래량 INT,
매도거래량 INT,
매도호가총잔량 INT,
매수호가총잔량 INT,
매도호가총잔량직전대비 INT,
매수호가총잔량직전대비 INT,
순매수잔량 INT,
매수비율 FLOAT(4),
순매도잔량 INT,
매도비율 FLOAT(4),
매도호가5 INT,
매도호가4 INT,
매도호가3 INT,
매도호가2 INT,
매도호가1 INT,
매수호가1 INT,
매수호가2 INT,
매수호가3 INT,
매수호가4 INT,
매수호가5 INT,
매도수량5 INT,
매도수량4 INT,
매도수량3 INT,
매도수량2 INT,
매도수량1 INT,
매수수량1 INT,
매수수량2 INT,
매수수량3 INT,
매수수량4 INT,
매수수량5 INT,
매도수량변화5 INT,
매도수량변화4 INT,
매도수량변화3 INT,
매도수량변화2 INT,
매도수량변화1 INT,
매수수량변화1 INT,
매수수량변화2 INT,
매수수량변화3 INT,
매수수량변화4 INT,
매수수량변화5 INT
)"""

CREATE_DELTA_BIDASK_TABLE = """
CREATE TABLE IF NOT EXISTS delta_bidask (
날짜 VARCHAR(8), 시간 VARCHAR(6), 수신시간 VARCHAR(15), 수신번호 INT, 종목코드 VARCHAR(255), 체결여부 INT,
현재가 INT, 시가 INT, 고가 INT, 저가 INT, 
등락율 FLOAT(4),
누적거래대금 INT,
체결강도 FLOAT(4),
거래대금증감 INT,
전일거래량대비 FLOAT(4),
거래회전율 FLOAT(4),
전일동시간거래량비율 FLOAT(4),
시가총액 FLOAT(4),
매수거래량 INT,
매도거래량 INT,
매도호가총잔량 INT,
매수호가총잔량 INT,
매도호가총잔량직전대비 INT,
매수호가총잔량직전대비 INT,
순매수잔량 INT,
매수비율 FLOAT(4),
순매도잔량 INT,
매도비율 FLOAT(4),
매도호가5 INT,
매도호가4 INT,
매도호가3 INT,
매도호가2 INT,
매도호가1 INT,
매수호가1 INT,
매수호가2 INT,
매수호가3 INT,
매수호가4 INT,
매수호가5 INT,
매도수량5 INT,
매도수량4 INT,
매도수량3 INT,
매도수량2 INT,
매도수량1 INT,
매수수량1 INT,
매수수량2 INT,
매수수량3 INT,
매수수량4 INT,
매수수량5 INT,
추적매도호가5 INT,
추적매도호가4 INT,
추적매도호가3 INT,
추적매도호가2 INT,
추적매도호가1 INT,
추적매수호가1 INT,
추적매수호가2 INT,
추적매수호가3 INT,
추적매수호가4 INT,
추적매수호가5 INT,
매도수량변화5 INT,
매도수량변화4 INT,
매도수량변화3 INT,
매도수량변화2 INT,
매도수량변화1 INT,
매수수량변화1 INT,
매수수량변화2 INT,
매수수량변화3 INT,
매수수량변화4 INT,
매수수량변화5 INT,
체결발생매도수량변화5 INT,
체결발생매도수량변화4 INT,
체결발생매도수량변화3 INT,
체결발생매도수량변화2 INT,
체결발생매도수량변화1 INT,
체결발생매수수량변화1 INT,
체결발생매수수량변화2 INT,
체결발생매수수량변화3 INT,
체결발생매수수량변화4 INT,
체결발생매수수량변화5 INT,
총매도대기금액 INT,
총매수대기금액 INT,
총매도대기금액변화 INT,
총매수대기금액변화 INT,
틱간매도금액 INT,
틱간매수금액 INT
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
SUM(CASE WHEN 체결거래량 > 0 THEN 체결거래량 ELSE 0 END) AS 초당매수수량,
SUM(CASE WHEN 체결거래량 < 0 THEN -1*체결거래량 ELSE 0 END) AS 초당매도수량,
SUM(CASE WHEN 체결거래량 > 0 THEN 현재가*체결거래량/1000000.0 ELSE 0.0 END) AS 초당매수거래대금,
SUM(CASE WHEN 체결거래량 < 0 THEN -1*체결거래량*현재가/1000000.0 ELSE 0.0 END) AS 초당매도거래대금,
SUM(CASE WHEN 체결거래량 < 0 THEN -1*체결거래량*현재가/1000000.0 ELSE 현재가*체결거래량/1000000.0 END) AS 초당거래대금,
COUNT(*) AS 틱개수
FROM (SELECT s.*, ROW_NUMBER() OVER() rn FROM settlement s) settlement_rn
GROUP BY 날짜, 시간, 종목코드
ORDER BY 종목코드 ASC, 시간 ASC
"""

CALC_SECOND_SETTLEMENT_WITH_RECEIVE_NO = """
with last_rows as (
select 날짜, 시간, 종목코드, 현재가 as 종가, 등락율, 누적거래대금, 체결강도, 거래대금증감, 거래회전율, 전일거래량대비, 전일동시간거래량비율, 시가총액, 수신번호 as 최종수신번호 from settlement where 수신번호 in (select max(수신번호) from settlement s group by 시간, 종목코드)
), first_rows as (
select 날짜, 시간, 종목코드, 현재가 as 시가 from settlement where 수신번호 in (select min(수신번호) from settlement s group by 시간, 종목코드)
), calc_rows as (
select 날짜, 시간, 종목코드, MAX(고가) AS 고가, MIN(저가) AS 저가, SUM(체결거래량) AS 체결거래량, COUNT(*) AS 틱개수,
SUM(CASE WHEN 체결거래량 > 0 THEN 체결거래량 ELSE 0 END) AS 초당매수수량,
SUM(CASE WHEN 체결거래량 < 0 THEN -1*체결거래량 ELSE 0 END) AS 초당매도수량,
SUM(CASE WHEN 체결거래량 > 0 THEN 현재가*체결거래량/1000000.0 ELSE 0.0 END) AS 초당매수거래대금,
SUM(CASE WHEN 체결거래량 < 0 THEN -1*체결거래량*현재가/1000000.0 ELSE 0.0 END) AS 초당매도거래대금,
SUM(CASE WHEN 체결거래량 < 0 THEN -1*체결거래량*현재가/1000000.0 ELSE 현재가*체결거래량/1000000.0 END) AS 초당거래대금
from settlement group by 시간, 종목코드
)
select 날짜, 시간, 종목코드, 시가, 종가, 고가, 저가, 등락율, 누적거래대금, 체결강도, 거래대금증감, 거래회전율, 전일거래량대비, 전일동시간거래량비율, 시가총액, 체결거래량, 초당매수수량, 초당매도수량, 초당매수거래대금, 초당매도거래대금, 초당거래대금, 틱개수, 최종수신번호
from last_rows
inner join calc_rows using(날짜, 시간, 종목코드)
inner join first_rows using(날짜, 시간, 종목코드)
order by 종목코드 asc, 시간 asc
"""

CALC_SECOND_BIDASK = """
WITH bidask_grouped_and_ranked AS (
  SELECT bn.*, 
  ROW_NUMBER() OVER (PARTITION BY 날짜, 시간, 종목코드 ORDER BY 수신번호 DESC) AS grouped_rn,
  ROW_NUMBER() OVER (PARTITION BY 날짜, 시간, 종목코드 ORDER BY 수신번호 ASC) AS record_count
  FROM bidask AS bn
), second_end_bidask AS (
  SELECT 날짜, 시간, 종목코드, record_count 틱개수, 수신번호 최종수신번호,
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

DETECT_CONSISTENCY_INJECTION_REQUIRED_ROW = """
-- 호가 있으면서 작업 필요한 것 먼저 작업. 이후 forward fill.이후 호가 없었으면서 작업 필요한 것 작업
with all_times as (
select 시간 from (select * from second_settlement where 종목코드 = '%s')
union
select 시간 from (select * from second_bidask where 종목코드 = '%s')
), need_processing as (
select 시간, 
SUM(CASE WHEN 체결거래량 > 0 THEN 체결거래량 ELSE 0 END) AS 매수체결거래량,
SUM(CASE WHEN 체결거래량 < 0 THEN -1*체결거래량 ELSE 0 END) AS 매도체결거래량,
호가최종수신번호
from (select * from settlement where 종목코드 = '%s') single
left join (select 시간, 최종수신번호 호가최종수신번호 from second_bidask where 종목코드 = '%s') using(시간)
where 수신번호 > 호가최종수신번호 or 호가최종수신번호 is null
group by 시간, 호가최종수신번호
)
select
case when 매수체결거래량 is not null and 호가최종수신번호 is not null then 1 when 매수체결거래량 is not null and 호가최종수신번호 is null then 2 else 0 end as 작업순서, *
from all_times
left join need_processing using(시간)
left join (select * from second_bidask where 종목코드 = '%s') using(시간)
"""

MERGE_SECOND_DATA = """
select 날짜, 시간, 종목코드, 종가 현재가, 시가, 고가, 저가, 등락율, 누적거래대금, 체결강도, 거래대금증감,
전일거래량대비, 거래회전율, 전일동시간거래량비율, 시가총액, 초당매수수량, 초당매도수량, 초당거래대금,
매도호가총잔량, 매수호가총잔량,
매도호가10, 매도호가9, 매도호가8, 매도호가7, 매도호가6, 매도호가5, 매도호가4, 매도호가3, 매도호가2, 매도호가1, 매수호가1, 매수호가2, 매수호가3, 매수호가4, 매수호가5, 매수호가6, 매수호가7, 매수호가8, 매수호가9, 매수호가10,
매도수량10, 매도수량9, 매도수량8, 매도수량7, 매도수량6, 매도수량5, 매도수량4, 매도수량3, 매도수량2, 매도수량1, 매수수량1, 매수수량2, 매수수량3, 매수수량4, 매수수량5, 매수수량6, 매수수량7, 매수수량8, 매수수량9, 매수수량10,
ss.틱개수 초당체결틱수, sb.틱개수 초당호가틱수
from (select * from second_settlement where 종목코드 = '%s') ss 
left join (select * from second_bidask where 종목코드 = '%s') sb using(날짜, 시간, 종목코드)
union
select 날짜, 시간, 종목코드, 종가 현재가, 시가, 고가, 저가, 등락율, 누적거래대금, 체결강도, 거래대금증감,
전일거래량대비, 거래회전율, 전일동시간거래량비율, 시가총액, 초당매수수량, 초당매도수량, 초당거래대금,
매도호가총잔량, 매수호가총잔량,
매도호가10, 매도호가9, 매도호가8, 매도호가7, 매도호가6, 매도호가5, 매도호가4, 매도호가3, 매도호가2, 매도호가1, 매수호가1, 매수호가2, 매수호가3, 매수호가4, 매수호가5, 매수호가6, 매수호가7, 매수호가8, 매수호가9, 매수호가10,
매도수량10, 매도수량9, 매도수량8, 매도수량7, 매도수량6, 매도수량5, 매도수량4, 매도수량3, 매도수량2, 매도수량1, 매수수량1, 매수수량2, 매수수량3, 매수수량4, 매수수량5, 매수수량6, 매수수량7, 매수수량8, 매수수량9, 매수수량10,
ss.틱개수 초당체결틱수, sb.틱개수 초당호가틱수
from (select * from second_bidask where 종목코드 = '%s') sb
left join (select * from second_settlement where 종목코드 = '%s') ss using(날짜, 시간, 종목코드)
order by 시간
"""

CONVERT_TO_STOM = """
select cast(날짜 || 시간 as integer) as 'index', 종가 현재가, 시가, 고가, 저가, 등락율, 누적거래대금 당일거래대금, 체결강도, 거래대금증감,
전일거래량대비 전일비, 거래회전율 회전율, 전일동시간거래량비율 전일동시간비, 시가총액, null 라운드피겨위5호가이내, 초당매수수량, 초당매도수량, 
null VI해제시간, null VI가격, null VI호가단위, 초당거래대금, 100*(종가*2.0/(고가+저가)-1.0) 고저평균대비등락율, 매도호가총잔량 매도총잔량, 매수호가총잔량 매수총잔량,
매도호가5, 매도호가4, 매도호가3, 매도호가2, 매도호가1, 매수호가1, 매수호가2, 매수호가3, 매수호가4, 매수호가5, 
매도수량5 매도잔량5, 매도수량4 매도잔량4, 매도수량3 매도잔량3, 매도수량2 매도잔량2, 매도수량1 매도잔량1, 매수수량1 매수잔량1, 매수수량2 매수잔량2, 매수수량3 매수잔량3, 매수수량4 매수잔량4, 매수수량5 매수잔량5,
매도수량5 + 매도수량4 + 매도수량3 + 매도수량2 + 매도수량1 + 매수수량1 + 매수수량2 + 매수수량3 + 매수수량4 + 매수수량5 매도수5호가잔량합,
ss.틱개수 초당체결틱수, sb.틱개수 초당호가틱수
from (select * from second_settlement where 종목코드 = '%s') ss 
left join (select * from second_bidask where 종목코드 = '%s') sb using(날짜, 시간, 종목코드)
union
select cast(날짜 || 시간 as integer) as 'index', 종가 현재가, 시가, 고가, 저가, 등락율, 누적거래대금 당일거래대금, 체결강도, 거래대금증감,
전일거래량대비 전일비, 거래회전율 회전율, 전일동시간거래량비율 전일동시간비, 시가총액, null 라운드피겨위5호가이내, 초당매수수량, 초당매도수량, 
null VI해제시간, null VI가격, null VI호가단위, 초당거래대금, 100*(종가*2.0/(고가+저가)-1.0) 고저평균대비등락율, 매도호가총잔량 매도총잔량, 매수호가총잔량 매수총잔량,
매도호가5, 매도호가4, 매도호가3, 매도호가2, 매도호가1, 매수호가1, 매수호가2, 매수호가3, 매수호가4, 매수호가5, 
매도수량5 매도잔량5, 매도수량4 매도잔량4, 매도수량3 매도잔량3, 매도수량2 매도잔량2, 매도수량1 매도잔량1, 매수수량1 매수잔량1, 매수수량2 매수잔량2, 매수수량3 매수잔량3, 매수수량4 매수잔량4, 매수수량5 매수잔량5,
매도수량5 + 매도수량4 + 매도수량3 + 매도수량2 + 매도수량1 + 매수수량1 + 매수수량2 + 매수수량3 + 매수수량4 + 매수수량5 매도수5호가잔량합,
ss.틱개수 초당체결틱수, sb.틱개수 초당호가틱수
from (select * from second_bidask where 종목코드 = '%s') sb
left join (select * from second_settlement where 종목코드 = '%s') ss using(날짜, 시간, 종목코드)
order by cast(날짜 || 시간 as integer)
"""

GENERATE_INDEX = """
Create Index if not exists ix_%s_index On "%s"('index');
"""

EXTRACT_DETAILED_TICK = """
with filtered_settlement as (select * from settlement s where 시간 >= '090000' and 종목코드 = '{}'),
filtered_bidask as (select * from bidask where 시간 >= '090000' and 종목코드 = '{}')
select 날짜, 시간, strftime('%H:%M:%f', 수신시간/1000000000.0, 'unixepoch', 'localtime') 수신시간, 수신번호, 
현재가, 시가, 고가, 저가, 등락율, 누적거래대금, 체결강도, 거래대금증감, 전일거래량대비, 거래회전율, 전일동시간거래량비율, 시가총액, 
CASE WHEN 체결거래량 > 0 THEN 체결거래량 ELSE 0 END 매수거래량,
CASE WHEN 체결거래량 > 0 THEN 0 ELSE -1*체결거래량 END 매도거래량,
null as 매도호가총잔량, null as 매수호가총잔량, null as 매도호가총잔량직전대비, null as 매수호가총잔량직전대비, null as 순매수잔량, null as 매수비율, null as 순매도잔량, null as 매도비율, 
null as 매도호가5, null as 매도호가4, null as 매도호가3, null as 매도호가2, null as 매도호가1, null as 매수호가1, null as 매수호가2, null as 매수호가3, null as 매수호가4, null as 매수호가5,
null as 매도수량5, null as 매도수량4, null as 매도수량3, null as 매도수량2, null as 매도수량1, null as 매수수량1, null as 매수수량2, null as 매수수량3, null as 매수수량4, null as 매수수량5
from filtered_settlement fs
union
select 날짜, 시간, strftime('%H:%M:%f', 수신시간/1000000000.0, 'unixepoch', 'localtime') 수신시간, 수신번호,
null as 현재가, null as 시가, null as 고가, null as 저가, null as 등락율, null as 누적거래대금, null as 체결강도, null as 거래대금증감,
null as 전일거래량대비, null as 거래회전율, null as 전일동시간거래량비율, null as 시가총액, null as  매수거래량, null as  매도거래량,
매도호가총잔량, 매수호가총잔량, 매도호가총잔량직전대비, 매수호가총잔량직전대비, 순매수잔량, 매수비율, 순매도잔량, 매도비율,
매도호가5, 매도호가4, 매도호가3, 매도호가2, 매도호가1, 매수호가1, 매수호가2, 매수호가3, 매수호가4, 매수호가5, 
매도수량5, 매도수량4, 매도수량3, 매도수량2, 매도수량1, 매수수량1, 매수수량2, 매수수량3, 매수수량4, 매수수량5
from filtered_bidask fb
order by 수신번호
"""