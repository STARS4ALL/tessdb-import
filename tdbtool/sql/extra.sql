-------------------------------
-- Auxiliar database DATA Model
-------------------------------

CREATE TABLE IF NOT EXISTS raw_readings_t
(
    name                TEXT    NOT NULL, -- TESS-W name
    rank                INTEGER NOT NULL, -- insertion order
    date_id             INTEGER NOT NULL, 
    time_id             INTEGER NOT NULL, 
    tstamp              TEXT    NOT NULL, -- ISO8601 timestamp
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
    --- These are filled in later in the process
    tess_id             INTEGER,
    location_id         INTEGER,
    units_id            INTEGER,
    --- Here start mnagamenet fields
    seconds             INTEGER NOT NULL, -- time_id as true seconds within the day
    line_number         INTEGER NOT NULL, -- original line number where dupliated appear
    rejected            TEXT,             -- Rejected reason 'Dup Sequence Number','Single','Couple', ...
    PRIMARY KEY(name, date_id, time_id)
);

CREATE INDEX IF NOT EXISTS raw_readings_i  ON raw_readings_t(rank);

-- These are detected when reading the CSV file to
-- the raw_readings trable
CREATE TABLE IF NOT EXISTS duplicated_readings_t
(
    name                TEXT    NOT NULL, -- TESS-W name
    rank                INTEGER NOT NULL, -- insertion order
    date_id             INTEGER NOT NULL, 
    time_id             INTEGER NOT NULL, 
    tstamp              TEXT    NOT NULL, -- ISO8601 timestamp
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
    line_number         INTEGER NOT NULL, -- original line number where dupliated appear
    file                TEXT,
    PRIMARY KEY(name, date_id, time_id)
);

-- This table helps save time when loading CSV files
CREATE TABLE IF NOT EXISTS housekeeping_t
(
    name                TEXT    , -- TESS-W name
    max_tstamp          TEXT    , -- max ISO8601 timestamp processed per TESS-W
    max_rank            INTEGER , -- max load counter id
    PRIMARY KEY(name)
);


CREATE TABLE IF NOT EXISTS first_differences_t
(
    name                TEXT    NOT NULL, -- TESS-W name
    date_id             INTEGER NOT NULL,
    time_id             INTEGER NOT NULL, -- final point of the difference
    tstamp              TEXT    NOT NULL, -- final point of the difference
    rank                INTEGER NOT NULL, -- final point of the difference
    delta_seq           INTEGER NOT NULL, -- sequence number difference
    delta_T        INTEGER NOT NULL, -- time difference in seconds
    period              REAL    NOT NULL,
    N                   INTEGER NOT NULL, -- sample count DO WE NEED IT ???
    control             INTEGER NOT NULL, -- control column. Should be 1.
    PRIMARY KEY(name, date_id, time_id)
);


CREATE TABLE IF NOT EXISTS daily_stats_t
(
	date_id             INTEGER NOT NULL, 
	name                TEXT    NOT NULL, -- TESS-W name
	median_period       REAL,	          -- median Tx period over a day
	mean_period         REAL,             -- average Tx period over a day
	stddev_period       REAL,             -- period stddev over a day
    min_period          REAL,             -- min period over a day
    max_period          REAL,             -- min period over a day
    N                   INTEGER NOT NULL, -- sample count where median was computed
	PRIMARY KEY (name, date_id)
);

CREATE TABLE IF NOT EXISTS global_stats_t
( 
    name                TEXT    NOT NULL, -- TESS-W name
    median_period       REAL,             -- overall median Tx period
    method              TEXT    NOT NULL, -- either 'Manual' or 'Automatic'
    N                   INTEGER NOT NULL, -- sample count where median was computed
    PRIMARY KEY (name)
);

CREATE TABLE IF NOT EXISTS location_daily_aggregate_t
( 
    tess_id             INTEGER NOT NULL, -- 
    date_id             INTEGER NOT NULL, --
    location_id         INTEGER NOT NULL, -- The location Id
    same_location       INTEGER NOT NULL, -- True if same location is maintained during a day
    PRIMARY KEY (tess_id, date_id)
);