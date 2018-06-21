-- Database: InsightDB

-- DROP DATABASE "InsightDB";

CREATE DATABASE "InsightDB"
    WITH 
    OWNER = sherry_jiayun
    ENCODING = 'UTF8'
    LC_COLLATE = 'en_US.UTF-8'
    LC_CTYPE = 'en_US.UTF-8'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1;


SELECT * FROM pg_catalog.pg_tables;

CREATE TABLE DATE_2008(
	time DATE,
	tech VARCHAR(50),
	appNum Integer,
	PRIMARY KEY (time, tech)
);
CREATE TABLE DATE_2009(
	time DATE,
	tech VARCHAR(50),
	appNum Integer,
	PRIMARY KEY (time, tech)
);
CREATE TABLE DATE_2010(
	time DATE,
	tech VARCHAR(50),
	appNum Integer,
	PRIMARY KEY (time, tech)
);
CREATE TABLE DATE_2011(
	time DATE,
	tech VARCHAR(50),
	appNum Integer,
	PRIMARY KEY (time, tech)
);
CREATE TABLE DATE_2012(
	time DATE,
	tech VARCHAR(50),
	appNum Integer,
	PRIMARY KEY (time, tech)
);
CREATE TABLE DATE_2013(
	time DATE,
	tech VARCHAR(50),
	appNum Integer,
	PRIMARY KEY (time, tech)
);
CREATE TABLE DATE_2014(
	time DATE,
	tech VARCHAR(50),
	appNum Integer,
	PRIMARY KEY (time, tech)
);
CREATE TABLE DATE_2015(
	time DATE,
	tech VARCHAR(50),
	appNum Integer,
	PRIMARY KEY (time, tech)
);
CREATE TABLE DATE_2016(
	time DATE,
	tech VARCHAR(50),
	appNum Integer,
	PRIMARY KEY (time, tech)
);
CREATE TABLE DATE_2017(
	time DATE,
	tech VARCHAR(50),
	appNum Integer,
	PRIMARY KEY (time, tech)
);
CREATE TABLE DATE_2018(
	time DATE,
	tech VARCHAR(50),
	appNum Integer,
	PRIMARY KEY (time, tech)
);

INSERT INTO DATE_2008 AS d_new (time,tech,appNum)
VALUES	('2008-01-01','c#',2),
		('2008-01-01','c++',5)
ON CONFLICT (time,tech) DO NOTHING;

UPDATE DATE_2008 
SET appNum = appNum + 1
WHERE time = '2008-01-01' AND tech = 'c#';

UPDATE DATE_2008 as d set 
	appNum = c.appNum + d.appNum
FROM (values 
	(date'2008-01-01','c#',3),
	(date'2008-01-01','c++',7)
) as c(time, tech, appNum)
WHERE c.time = d.time and c.tech = d.tech;

SELECT * FROM DATE_2008;










