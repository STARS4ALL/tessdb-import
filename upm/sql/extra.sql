-------------------------------
-- Auxiliar database DATA Model
-------------------------------

CREATE TABLE IF NOT EXISTS raw_readings_t
(
    date_id             INTEGER NOT NULL, 
    time_id             INTEGER NOT NULL, 
    tess                TEXT    NOT NULL,
    sequence_number     INTEGER NOT NULL,
    frequency           REAL    NOT NULL,
    magnitude           REAL    NOT NULL,
    ambient_temperature REAL    NOT NULL,
    sky_temperature     REAL    NOT NULL,
    signal_strength     INTEGER NOT NULL,
    azimuth             REAL,
    altitude            REAL,
    longitude           REAL,
    latitude            REAL,
    height              REAL,
    --- Here start mnagamenet fields
    retained            INTEGER DEFAULT 0,
    duplicated          INTEGER DEFAULT 0,
    PRIMARY KEY (date_id, time_id, tess)
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