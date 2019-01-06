
-- Delete Intermissions from concert_selection where intermission is last

create temporary table extraneous_intermission_cs_records AS
select
      concert_id,
      max(concert_order) as max_ord,
      max(case when selection_id=4 then concert_order else null end) as int_ord,
      max(case when selection_id=4 then id else null end) as int_id
from concert_selection
group by 1
having max_ord=int_ord and int_ord is not null;

delete from concert_selection where id in (select int_id from extraneous_intermission_cs_records);
