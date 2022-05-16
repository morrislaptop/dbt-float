SELECT
	ds.date_day::date,
	t.people_id,
	t.project_id,
	p.department__name,
	t.hours
FROM
	{{ ref('date_spine') }} ds
	JOIN singer.tasks t ON ds.date_day BETWEEN t.start_date::date AND t.end_date::date
	JOIN singer.people p ON t.people_id = p.people_id
