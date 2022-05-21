-- tblClients - информация о клиенте
-- ClientID | Status
-- ClientID – идентификатор клиента
-- Status – статус клиента (строка)

-- tblTestClients - информация о клиенте (табличка с аналогичной структурой с тестовыми данными, их нужно выкидывать)

-- tblClientBalanceOperation
-- ClientID | OperationTime | Amount | SignOfPayment | 
-- OperationTime – время покупки
-- Amount – сумма покупки
-- SignOfPayment – 1/0, признак успешности операции

-- 2.1 Вывести детализацию по клиенту:
-- a. ID клиента
-- b. Дата и сумма первой покупки
-- c. Дата и сумма повторной (следующей после первой) покупки
-- d. Дата последней покупки
-- e. Сумма покупок, совершенных в течение месяца после первой покупки
-- f. Время (кол-во дней) между первой и повторной покупкой
-- g. Среднее время (кол-во дней) между покупками

with raw_data as(
    select cl."ClientID"
    	, bo."OperationTime"
    	, rank() over (partition by cl."ClientID" order by bo."OperationTime") as "OperationRank"
    	, extract(epoch from bo."OperationTime" - lag(bo."OperationTime") 
    		over (partition by cl."ClientID" order by bo."OperationTime"))/86400 
    	    as interval_days
    	, min(bo."OperationTime") over (partition by cl."ClientID") as fisrt_pur_date
    	, max(bo."OperationTime") over (partition by cl."ClientID") as last_pur_date
    	, bo."Amount"
    from "tblClients" cl 
    left join "tblTestClients" tcl on tcl."ClientID" = cl."ClientID"
    left join "tblClientBalanceOperation" bo on bo."ClientID" = cl."ClientID"
    where tcl."ClientID" is null
        and bo."SignOfPayment"=1
)
select distinct "ClientID"
    , fisrt_pur_date
    , (select "OperationTime" from raw_data ir where ir."ClientID" = r."ClientID" and "OperationRank" = 2) 
    	as next_pur_date
    , last_pur_date
    , sum(case when "OperationTime" < fisrt_pur_date + interval '1 month' then "Amount" else 0 end) 
    	as purs_amount
    , extract(epoch from last_pur_date - fisrt_pur_date)/86400 as interval_first_last
    , avg(interval_days) as mean_int
from raw_data r 
group by "ClientID", fisrt_pur_date, last_pur_date