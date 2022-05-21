-- Произведение
-- | id | Название |


-- Издание
-- | id | id_произведения | год_издания | кол-во_страниц |


-- Экземпляр	
-- | id | id_издание | инвентаризационный_номер |


-- Лог операций
-- | id | id_user | id_экземпляр | дата_взяли | дата_вернули |



-- Для каждого пользователя найти последние три взятые им произведения.
-- Для каждого такого произведения указать сколько всего раз его брали (за все время).

with order_data as( 
    select distinct log.id_user
    	, pr."Название" 
    	, rank() over (partition by log.id_user order by log.дата_взяли desc) as порядок
    	, count(log.id) over(partition by pr."Название") as количество_брали
    from "Лог операций" log
    join "Экземпляр" copy on copy.id = log.id_экземпляр
    join "Издание" issue on issue.id = copy.id_издание
    join "Произведение" pr on pr.id = issue.id_произведения
)
select id_user, "Название", количество_брали
from order_data
where порядок <= 3