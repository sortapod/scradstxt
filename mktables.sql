CREATE TABLE site(
siteid serial primary key,
siteaddr varchar(255),
arank integer);

CREATE TABLE scanresult(
scanid serial primary key,
siteid integer REFERENCES site (siteid),
scandt timestamp,
result smallint,
resphash bytea);

CREATE TABLE advendor(
avid serial primary key,
advendorname varchar(255));

CREATE TABLE publisher(
pavid integer REFERENCES advendor (avid),
siteid integer REFERENCES site (siteid),
publisherid varchar(255));
