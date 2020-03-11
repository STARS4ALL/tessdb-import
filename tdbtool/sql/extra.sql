-------------------------------
-- Auxiliar database DATA Model
-------------------------------

CREATE TABLE IF NOT EXISTS raw_readings_t
(
    rank                INTEGER NOT NULL, -- insertion order
    date_id             INTEGER NOT NULL, 
    time_id             INTEGER NOT NULL, 
    tess                TEXT    NOT NULL,
    sequence_number     INTEGER NOT NULL,
    frequency           REAL    NOT NULL,
    magnitude           REAL    NOT NULL,
    ambient_temperature REAL    NOT NULL,
    sky_temperature     REAL    NOT NULL,
    signal_strength     INTEGER         ,
    azimuth             REAL,
    altitude            REAL,
    longitude           REAL,
    latitude            REAL,
    height              REAL,
    --- Here start mnagamenet fields
    seconds             INTEGER NOT NULL, -- time_id as true seconds within the day
    tstamp              INTEGER NOT NULL, -- Combined date_id + time_id as integer
    line_number         INTEGER NOT NULL, --original line number where dupliated appear
    rejected            TEXT,             -- Rejected reason 'Dup Sequence Number','Single','Couple', ...
    PRIMARY KEY(date_id, time_id, tess)
);

-- These are detected when reading the CSV file to
-- the raw_readings trable
CREATE TABLE IF NOT EXISTS duplicated_readings_t
(
    rank                INTEGER NOT NULL, -- insertion order
    date_id             INTEGER NOT NULL, 
    time_id             INTEGER NOT NULL, 
    tess                TEXT    NOT NULL,
    sequence_number     INTEGER NOT NULL,
    frequency           REAL    NOT NULL,
    magnitude           REAL    NOT NULL,
    ambient_temperature REAL    NOT NULL,
    sky_temperature     REAL    NOT NULL,
    signal_strength     INTEGER         ,
    azimuth             REAL,
    altitude            REAL,
    longitude           REAL,
    latitude            REAL,
    height              REAL,
     --- Here start mnagamenet fields
    seconds             INTEGER NOT NULL, -- time_id as true seconds within the day
    tstamp              INTEGER NOT NULL, -- Combined date_id + time_id as integer
    line_number         INTEGER NOT NULL, --original line number where dupliated appear
    file                TEXT,
    iso8601             TEXT,             -- ISO 8601 timestamp string
    PRIMARY KEY(date_id, time_id, tess)
);

-- This table helps save time when loading CSV files
CREATE TABLE IF NOT EXISTS housekeeping_t
(
    tess                TEXT    ,
    max_tstamp          INTEGER , -- max timestamp processed per TESS-W
    max_rank            INTEGER , -- max load counter id
    PRIMARY KEY(tess)
);


CREATE TABLE IF NOT EXISTS first_differences_t
(
    tess                TEXT    NOT NULL,
    date_id             INTEGER NOT NULL,
    time_id             INTEGER NOT NULL, -- final point of the difference
    rank                INTEGER NOT NULL, -- final point of the difference
    seq_diff            INTEGER NOT NULL,
    seconds_diff        INTEGER NOT NULL,
    period              REAL    NOT NULL,
    N                   INTEGER NOT NULL, -- sample count DO WE NEED IT ???
    control             INTEGER NOT NULL, -- control column. Should be 1.
    PRIMARY KEY(tess, date_id, time_id)
);


CREATE TABLE IF NOT EXISTS stats_t
(
	date_id             INTEGER NOT NULL, 
	tess                TEXT    NOT NULL,
	median_period       REAL,	-- median Tx period over a day
	mean_period         REAL,   -- median Tx period over a day
	stddev_period       REAL,   -- period stddev over a day
	PRIMARY KEY (date_id, tess)
);