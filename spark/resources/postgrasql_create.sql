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

CREATE TABLE insight.TECH_NODE(
	technode VARCHAR(50),
	weight Integer,
	count Integer,
	PRIMARY KEY (technode)
);

CREATE TABLE insight.TECH_REL(
	technode1 VARCHAR(50),
	technode2 VARCHAR(50),
	weight Integer,
	count Integer,
	PRIMARY KEY (technode1,technode2)
);

CREATE TABLE insight.USER_TECH(
	userId Integer,
	userName VARCHAR(100),
	userWebSite VARCHAR(100),
	tech VARCHAR(100),
	score Integer,
	count Integer,
	PRIMARY KEY (userId,tech)
);

CREATE TABLE insight.DATE_2008(
	time DATE,
	tech VARCHAR(50),
	appNum Integer,
	PRIMARY KEY (time, tech)
);

CREATE TABLE insight.DATE_2009(
	time DATE,
	tech VARCHAR(50),
	appNum Integer,
	PRIMARY KEY (time, tech)
);

CREATE TABLE insight.DATE_2010(
	time DATE,
	tech VARCHAR(50),
	appNum Integer,
	PRIMARY KEY (time, tech)
);

CREATE TABLE insight.DATE_2011(
	time DATE,
	tech VARCHAR(50),
	appNum Integer,
	PRIMARY KEY (time, tech)
);

CREATE TABLE insight.DATE_2012(
	time DATE,
	tech VARCHAR(50),
	appNum Integer,
	PRIMARY KEY (time, tech)
);

CREATE TABLE insight.DATE_2013(
	time DATE,
	tech VARCHAR(50),
	appNum Integer,
	PRIMARY KEY (time, tech)
);

CREATE TABLE insight.DATE_2014(
	time DATE,
	tech VARCHAR(50),
	appNum Integer,
	PRIMARY KEY (time, tech)
);

CREATE TABLE insight.DATE_2015(
	time DATE,
	tech VARCHAR(50),
	appNum Integer,
	PRIMARY KEY (time, tech)
);

CREATE TABLE insight.DATE_2016(
	time DATE,
	tech VARCHAR(50),
	appNum Integer,
	PRIMARY KEY (time, tech)
);

CREATE TABLE insight.DATE_2017(
	time DATE,
	tech VARCHAR(50),
	appNum Integer,
	PRIMARY KEY (time, tech)
);

CREATE TABLE insight.DATE_2018(
	time DATE,
	tech VARCHAR(50),
	appNum Integer,
	PRIMARY KEY (time, tech)
);










