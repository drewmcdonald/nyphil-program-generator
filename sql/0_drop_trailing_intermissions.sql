-- Delete Intermissions from concert_selection where intermission is last
-- This appears to be a notation of a long, multi-part piece played in its
-- entirety with an intermission between movements
WITH wide AS (
	SELECT
		cs.id,
        cs.concert_order,
		max(cs.concert_order) OVER (partition by cs.concert_id) as max_concert_order,
        w.title as work_title
	FROM concert_selection cs
		inner join selection s on cs.selection_id=s.id
		inner join `work` w on s.work_id=w.id
),
bad_ids AS (
	select id
	from wide
	where
		work_title='Intermission'
		AND concert_order = max_concert_order
)
DELETE from concert_selection where id in (select id from bad_ids);