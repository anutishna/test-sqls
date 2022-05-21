-- Потерянным клиентом считается тот, который совершал покупки в предыдущем месяце, 
-- но не совершал в текущем. Какой % из этих потерянных клиентов находятся в статусе Deleted?

with raw_data as(
    select cl."Status", 
        100*(count(cl."ClientID") over(partition by cl."Status"))::float/(count(cl."ClientID") over())::float 
        	as percentage
    from "tblClients" cl 
    left join "tblTestClients" tcl on tcl."ClientID" = cl."ClientID"
    where tcl."ClientID" is null
        -- были покупки в прошлом месяце:
        and exists (select 1 from "tblClientBalanceOperation" bo where bo."ClientID" = cl."ClientID" 
            and date_part('year', bo."OperationTime") = date_part('year', now() - interval '1 month') 
    	    and date_part('month', bo."OperationTime") = date_part('month', now() - interval '1 month')
            and bo."SignOfPayment"=1)
    	-- не было покупок в этом месяце:
    	and not exists (select 1 from "tblClientBalanceOperation" bo where bo."ClientID" = cl."ClientID"
    	    and date_part('year', bo."OperationTime") = date_part('year', now()) 
    	    and date_part('month', bo."OperationTime") = date_part('month', now())
            and bo."SignOfPayment"=1)
)
select percentage from raw_data where "Status" = 'Deleted'