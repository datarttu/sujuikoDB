/*
 * It seems that there are multiple messages with same timestamps
 * but different event types.
 * 1) Is VP always one of the events?
 * 2) Which events occur together?
 * 3) In overlapping VP & other records, are position fields always indifferent from those of the VP record?
 *
 * Testing with a subset of one day and three routes.
 */

WITH
  aggregates AS (
    SELECT jrnid, tst, array_agg(event_type ORDER BY event_type) AS events
    FROM stage_hfp.raw
    GROUP BY jrnid, tst
  ),
  combos AS (
    SELECT DISTINCT events
    FROM aggregates
  )
SELECT
  events @> '{VP}' AS has_vp,
  events
FROM combos
ORDER BY has_vp, array_length(events, 1) DESC;

-- There are more than 100 different event combos, and VP is not part of all.

/*
 * - Check if the following assumptions hold:
 *   - DOO is always together with the timestamp of first "drst is true" in a partition
 *   - DOC similarly but with the last timestamp
 *   - DEP, ARS and PAS are always related to a non-null stop value
 *   - There are never two or more successive DEP or ARS (they occur alternately)
 */
