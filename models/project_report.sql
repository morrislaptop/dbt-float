{% set timeoff = "COALESCE(tot.hours, 0)" %}
{% set net_scheduled = "GREATEST(pt.hours - " ~ timeoff ~ ", 0)" %}
{% set logged = "lt.hours" %} -- leave as null

SELECT
	{{ dbt_utils.surrogate_key(['pt.date_day', 'pt.project_id', 'pt.people_id']) }} AS id,
	pt.date_day AS date,
	pt.project_id,
	pt.people_id,
	pt.department__name,
	pt.hours AS scheduled,
	{{ timeoff }} AS timeoff,
	{{ net_scheduled }} AS net_scheduled,
	{{ logged }} AS logged,
	pr.hourly_rate,
	ROUND(({{ net_scheduled }} * hourly_rate)::numeric, 2) AS net_scheduled_budget,
	ROUND(({{ logged }} * hourly_rate)::numeric, 2) AS logged_budget
FROM
	{{ ref('date_spine') }} ds
	JOIN {{ ref('project_time') }} pt ON ds.date_day = pt.date_day
	LEFT JOIN {{ ref('timeoff_time') }} tot ON ds.date_day = tot.date_day AND pt.people_id = tot.people_id
	LEFT JOIN {{ ref('project_rates') }} pr ON pr.project_id = pt.project_id AND pr.people_id = pt.people_id
	LEFT JOIN singer.logged_time lt ON ds.date_day = lt.date::date AND lt.project_id = pt.project_id AND lt.people_id = pt.people_id
