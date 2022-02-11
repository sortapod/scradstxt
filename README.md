# Collects and parses ads.txt
GoLang program scrapes sites for ads.txt and stores its significant details to PostgreSQL database.

Give it a file with CSV list of sites to check (rank,site.url). I use top 1M sites from https://tranco-list.eu/top-1m.csv.zip For demonstration smaller `top-1k.csv` is supplied.

Scraper first checks HTTPS schema, if connection fails then fallback to HTTP. User-agent is spoofed. Timeout is 5 sec defined by const crawlerTimeout.

User who runs this program must have a ROLE in PostgreSQL allowing SELECT, INSERT, DELETE queries on working database. Program connects to the database via unix socket. Adjust connection string in sources if TCP or another DB name or another authentication method used.
PostgreSQL database is named adstxt.
```sh
sudo -u postgres psql -c 'CREATE DATABASE adstxt'
```
Create tables in it with the mktables.sql script.
```sh
psql -d adstxt < mktables.sql
```
Run the program with
```sh
go run main.go top-1k.csv
```
or build executable first
```sh
go build main.go
./main top-1k.csv
```
By default 64 goroutines run to fetch ads.txt from sites. This number can be increased for fast machines on fast connections with optional argument after the file name.
```sh
go run main.go top-1k.csv 1000
```
