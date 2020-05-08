CREATE TABLE stage_gtfs.service_dates (
 service_id    text      PRIMARY KEY,
 dates         date[]
);
COMMENT ON TABLE stage_gtfs.service_dates IS
'Operation days of service ids, based on gtfs calendar and calendar_dates.';

CREATE OR REPLACE FUNCTION stage_gtfs.populate_service_dates()
RETURNS TEXT
LANGUAGE PLPGSQL
VOLATILE
AS $$
DECLARE
 cnt       integer;
 mindate   date;
 maxdate   date;
BEGIN
 DELETE FROM stage_gtfs.service_dates;
 GET DIAGNOSTICS cnt = ROW_COUNT;
 RAISE NOTICE '% rows deleted from stage_gtfs.service_dates', cnt;

 SELECT INTO mindate min(start_date)
 FROM stage_gtfs.calendar;
 SELECT INTO maxdate max(end_date)
 FROM stage_gtfs.calendar;
 RAISE NOTICE 'Date range in stage_gtfs.calendar: % ... %', mindate, maxdate;

 WITH
   /*
    * Pivot "wide" table with days of week in cols
    * into "long" table with rows for valid dows only.
    */
   dows AS (
   	SELECT service_id, 0 AS dow, start_date, end_date
   	FROM stage_gtfs.calendar WHERE sunday IS TRUE
   	UNION
   	SELECT service_id, 1 AS dow, start_date, end_date
   	FROM stage_gtfs.calendar WHERE monday IS TRUE
   	UNION
   	SELECT service_id, 2 AS dow, start_date, end_date
   	FROM stage_gtfs.calendar WHERE tuesday IS TRUE
   	UNION
   	SELECT service_id, 3 AS dow, start_date, end_date
   	FROM stage_gtfs.calendar WHERE wednesday IS TRUE
   	UNION
   	SELECT service_id, 4 AS dow, start_date, end_date
   	FROM stage_gtfs.calendar WHERE thursday IS TRUE
   	UNION
   	SELECT service_id, 5 AS dow, start_date, end_date
   	FROM stage_gtfs.calendar WHERE friday IS TRUE
   	UNION
   	SELECT service_id, 6 AS dow, start_date, end_date
   	FROM stage_gtfs.calendar WHERE saturday IS TRUE
   ),
   /*
    * Construct a full set of individual dates so we can make weekday validities
    * into actual dates.
    */
   alldates AS (
   	SELECT
       generate_series::date AS date,
       extract(DOW FROM generate_series) AS dow
   	FROM generate_series(mindate::timestamp, maxdate::timestamp, '1 day'::interval)
   ),
   validdates AS (
     /*
   	 * Now the service_id is repeated for every single date on which it is valid.
      */
     SELECT dows.service_id, alldates.date, alldates.dow
   	FROM alldates
   		INNER JOIN dows
   		ON alldates.date <@ daterange(dows.start_date, dows.end_date + 1)
   		AND alldates.dow = dows.dow
   	/*
      * gtfs calendar_dates table format suits our needs without wrangling
      * between wide and long data.
      * Add calendar_dates with ADDED service.
      */
   	UNION
   	SELECT service_id, date, extract(DOW FROM date) AS dow
   	FROM stage_gtfs.calendar_dates
   	WHERE exception_type = 1
     /*
   	 * Remove calendar_dates with REMOVED service
      */
   	EXCEPT
   	SELECT service_id, date, extract(DOW FROM date) AS dow
   	FROM stage_gtfs.calendar_dates
   	WHERE exception_type = 2
   	ORDER BY date
   )
   INSERT INTO stage_gtfs.service_dates (service_id, dates)
   SELECT service_id, array_agg(date) AS dates
   FROM validdates
   GROUP BY service_id
   ORDER BY service_id;
   GET DIAGNOSTICS cnt = ROW_COUNT;
   RAISE NOTICE '% rows inserted into stage_gtfs.service_dates', cnt;

   RETURN 'OK';
END;
$$;
