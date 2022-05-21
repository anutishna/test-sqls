-- Сколько клиентов совершают первую покупку в первую неделю с регистрации на сайте, 
-- сколько во вторую неделю, сколько на третьей неделе и сколько позже? 
-- (вывести абсолютные значения и доли)

with raw_data as(
    select distinct cl."ClientID"
        , bo."OperationTime"
        , (case when bo."OperationTime" < (min(s."OnlineTime") over (partition by cl."ClientID") + interval '1 week') 
                then 'week_1_purchase'
            when bo."OperationTime" < (min(s."OnlineTime") over (partition by cl."ClientID") + interval '2 weeks')
                then 'week_2_purchase'
            when bo."OperationTime" < (min(s."OnlineTime") over (partition by cl."ClientID") + interval '3 weeks') 
                then 'week_3_purchase'
            when bo."OperationTime" >= (min(s."OnlineTime") over (partition by cl."ClientID") + interval '3 weeks')
                then 'later_purchase'
            else null end) as purchase_type
    from "tblClients" cl 
    left join "tblTestClients" tcl on tcl."ClientID" = cl."ClientID"
    left join "tblClientBalanceOperation" bo on bo."ClientID" = cl."ClientID"
    left join "tblOnlineSessions_mini" s on s."ClientID" = cl."ClientID"
    where tcl."ClientID" is null
        and bo."SignOfPayment"=1
),
all_clients as (select count(distinct "ClientID")::float as clients from raw_data)
select distinct purchase_type
    , count(distinct "ClientID") as clients
    , count(distinct "ClientID")::float / clients as client_share
from raw_data, all_clients
group by purchase_type, clients