create view composer_concert_selection_counts as
select
       id as composer_id,
       n_concertselections,
       case
         when n_concertselections >= 1000 then 'E: 1,000+'
         when n_concertselections >= 100  then 'D: 100-999'
         when n_concertselections >= 10   then 'C: 10-99'
         when n_concertselections >= 2    then 'B: 2-9'
         when n_concertselections =  1    then 'A: Only 1'
       end as n_concertselections_grp
from (
  select
         c.id,
         count(cs.id) as n_concertselections
  from concert_selection cs
    inner join selection s on cs.selection_id = s.id
    inner join work w on s.work_id = w.id
    inner join composer c on w.composer_id = c.id
    inner join concert c2 on cs.concert_id = c2.id
    inner join eventtype e on c2.eventtype_id = e.id
  where e.is_modelable
  group by c.id, c.name
) z
;

create view selection_performance_counts as
select
       selection_id,
       n_performances,
       case
         when n_performances >= 1000 then 'E: 1,000+'
         when n_performances >= 100  then 'D: 100-999'
         when n_performances >= 10   then 'C: 10-99'
         when n_performances >= 2    then 'B: 2-9'
         when n_performances =  1    then 'A: Only 1'
       end as n_performances_grp
from (
  select
    cs.selection_id
    , count(*) as n_performances
  from concert_selection cs
    inner join concert c on cs.concert_id = c.id
    inner join eventtype e on c.eventtype_id = e.id
  where e.is_modelable
  group by cs.selection_id
) z
;

create view selection_position_stats as
select selection_id,
       case
         when perc_after_intermission = 0.0 then 'A: Always Before'
         when perc_after_intermission = 1.0 then 'C: Always After'
         else 'B: Mixed'
       end as perc_after_intermission_bin,
       case
         when avg_perc_of_concert < .4 then 'A: 00-39'
         when avg_perc_of_concert < .6 then 'B: 40-59'
         when avg_perc_of_concert < .8 then 'C: 60-79'
         when avg_perc_of_concert >= .8 then 'D: 80-100'
       end as avg_perc_of_concert_bin
from (
	with stats as (
	    with concert_intermission_ord as (
	      select concert_id,
	             concert_order as intermission_ord
	      from concert_selection cs
	      where selection_id = 4
	      ),

	      concert_length as (
	        select concert_id,
	               max(concert_order) as concert_length
	        from concert_selection cs
	        group by 1
	        )
	      select cs.concert_id,
	             cs.selection_id,
	             concert_order > ifnull(i.intermission_ord, 0)                as after_intermission,
	             1.0 * ifnull(i.intermission_ord, 0) / concert_length         as intermission_perc_of_concert,
	             1.0 * concert_order / l.concert_length                       as perc_of_concert,
	             (1.0 * concert_order / l.concert_length)
	               - (1.0 * ifnull(i.intermission_ord, 0) / l.concert_length) as perc_of_concert_rel_intermission
	      from concert_selection cs
	             inner join concert c on cs.concert_id = c.id
	             inner join eventtype e on c.eventtype_id = e.id
	             left join concert_intermission_ord i on cs.concert_id = i.concert_id
	             left join concert_length l on cs.concert_id = l.concert_id
	      where cs.selection_id != 4
	        AND e.is_modelable
	    )
	    select selection_id,
	           round(avg(after_intermission), 2)               as perc_after_intermission,
	           round(avg(perc_of_concert), 2)                  as avg_perc_of_concert,
	           round(avg(perc_of_concert_rel_intermission), 2) as avg_perc_of_concert_rel_intermission
	    from stats
	    group by selection_id
	) z
;

