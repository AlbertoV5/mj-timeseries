-- Issue: Events from a specific date had events with the same alert_id under the same timestamp_id.
-- find one case
SELECT DISTINCT ON (alert_id) * FROM events WHERE timestamp_id = '2023-05-21 00:00:44' GROUP BY (date_id, timestamp_id, alert_id, date) ORDER BY alert_id, date DESC;
-- find all cases into new table
SELECT DISTINCT ON (timestamp_id, alert_id) * INTO events2 FROM events GROUP BY (date_id, timestamp_id, alert_id, date) ORDER BY timestamp_id, alert_id, date DESC;
-- compare
SELECT * FROM events2 WHERE timestamp_id = '2023-05-21 00:00:44';
SELECT * FROM events WHERE timestamp_id = '2023-05-21 00:00:44';
-- verify change in selected date
SELECT COUNT(*) FROM events WHERE date_id = '2023-05-21';
SELECT COUNT(*) FROM events2 WHERE date_id = '2023-05-21';
-- verify integrity of other dates
SELECT COUNT(*) FROM events WHERE date_id = '2023-05-20';
SELECT COUNT(*) FROM events2 WHERE date_id = '2023-05-20';
-- select all (timestamp_id, date) from events that are not in events2
SELECT COUNT(*) FROM events WHERE (timestamp_id, date) NOT IN (SELECT timestamp_id, date FROM events2);
SELECT timestamp_id, date FROM events WHERE (timestamp_id, date) NOT IN (SELECT timestamp_id, date FROM events2);
-- make backup events table
SELECT * INTO events_bk FROM events;
-- delete all items from events that are not in events2
DELETE FROM events WHERE (timestamp_id, date) NOT IN (SELECT timestamp_id, date FROM events2);
-- drop events2
DROP TABLE events2;
-- all good thank you :)