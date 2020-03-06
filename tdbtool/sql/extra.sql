-------------------------------
-- Auxiliar database DATA Model
-------------------------------

CREATE TABLE IF NOT EXISTS raw_readings_t
(
    id                  INTEGER NOT NULL,
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
    seconds             INTEGER NOT NULL,
    retained            INTEGER DEFAULT 0,
    duplicated          INTEGER DEFAULT 0,
    PRIMARY KEY(date_id, time_id, tess)
);

CREATE TABLE IF NOT EXISTS first_differences_t
(
    tess                TEXT    NOT NULL,
    date_id             INTEGER NOT NULL,
    time_id             INTEGER NOT NULL, -- final point of the difference
    id                  INTEGER NOT NULL, -- final point of the difference
    seq_diff            INTEGER NOT NULL,
    seconds_diff        INTEGER NOT NULL,
    period              REAL    NOT NULL,
    PRIMARY KEY(date_id, time_id, tess)
);


CREATE TABLE IF NOT EXISTS stats_t
(
	date_id             INTEGER NOT NULL REFERENCES date_t(date_id), 
	tess                TEXT    NOT NULL,
	median_period       REAL,	-- median Tx period over a day
	mean_period         REAL,   -- median Tx period over a day
	stddev_period       REAL,   -- period stddev over a day
	PRIMARY KEY (date_id, tess)
);