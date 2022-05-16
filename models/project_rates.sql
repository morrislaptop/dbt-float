SELECT
	ppl.people_id,
	t._sdc_source_key_project_id AS project_id,
	COALESCE(t.hourly_rate, ppl.default_hourly_rate) AS hourly_rate
FROM
	singer.people ppl
	LEFT JOIN singer.projects__project_team t ON t.people_id = ppl.people_id
