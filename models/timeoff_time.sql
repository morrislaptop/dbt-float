SELECT
	ds.date_day::date,
	top._sdc_value AS people_id,
	toff.full_day * 8 + COALESCE(toff.hours, 0) AS hours
FROM
	{{ ref('date_spine') }} ds
	LEFT JOIN singer.timeoffs toff ON ds.date_day BETWEEN toff.start_date::date AND toff.end_date::date
	LEFT JOIN singer.timeoffs__people_ids top ON toff.timeoff_id = top._sdc_source_key_timeoff_id
