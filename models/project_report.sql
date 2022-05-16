SELECT
	pt.date_day,
	pt.project_id,
	pt.people_id,
	pt.department__name,
-- 	pt.hours AS scheduled,
-- 	tot.hours AS timeoff,
	GREATEST(pt.hours - COALESCE(tot.hours, 0), 0) AS worked,
	COALESCE(lt.hours, 0) AS logged,
	pr.hourly_rate
FROM
	{{ ref('date_spine') }} ds
	JOIN {{ ref('project_time') }} pt ON ds.date_day = pt.date_day
	LEFT JOIN {{ ref('timeoff_time') }} tot ON ds.date_day = tot.date_day AND pt.people_id = tot.people_id
	LEFT JOIN {{ ref('project_rates') }} pr ON pr.project_id = pt.project_id AND pr.people_id = pt.people_id
	LEFT JOIN singer.logged_time lt ON ds.date_day = lt.date::date AND lt.project_id = pt.project_id AND lt.people_id = pt.people_id
