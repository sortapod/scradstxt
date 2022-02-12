package main

import (
	"fmt"
	"os"
	"io"
	"bytes"
	"sync"
	"strings"
	"strconv"
	"regexp"
	"net/url"
	"net"
	"bufio"
	"time"
	"io/ioutil"
	"net/http"
	"database/sql"
	"hash/fnv"
	_ "github.com/lib/pq"	
)

const (
	rrBadSrvName = -2
	rrNeitherProtocolResponse = -1
	minCrawlersCount = 4
	defaultCrawlersCount = 64
	crawlerTimeout = 5 //seconds
	dbConnectionString = "host=/var/run/postgresql dbname=adstxt"  // use unix socket for connection
)

type ReqRes struct {
	site Site
	status int
	respBody []byte
}

type Publisher struct {
	publisherid string
	advid int
	advname string
}

type Site struct {
	siteid int
	site string
}

func parseAdsLine(line string) (Publisher, bool) {
	r := Publisher{"", 0, ""}
	
	if commentStart := strings.Index(line, "#"); commentStart >= 0 {
		line = line[:commentStart]
	}
	line = strings.TrimSpace(line)
	
	if len(line) < 10 { //valid string must be at least: a,b,DIRECT
		return r, false
	}
	
	parts := strings.Split(line, ",")
	
	if len(parts) < 3 {
		return r, false
	}
	
	if p2 := strings.TrimSpace(parts[2]); p2 != "DIRECT" && p2 != "RESELLER" {
		return r, false
	}
	
	re := regexp.MustCompile(`[a-z0-9.-]*`) //valid domain name chars
	re.Longest()
	adVendor := re.FindString(strings.ToLower(strings.TrimSpace(parts[0])))
	reASCIIPrintable := regexp.MustCompile(`[\x21-\x7F]*`)//drop "strange" ids
	publisherId := reASCIIPrintable.FindString(strings.TrimSpace(parts[1]))
	
	if len(adVendor) == 0 || len(adVendor) > 70 || len(publisherId) == 0 || len(publisherId) > 70{
		return r, false
	}
	
	return Publisher{publisherId, 0, adVendor}, true
}

func getAds(sites chan Site, res chan ReqRes, wg * sync.WaitGroup) {
	for site := range sites {
		urlPrefix := [2]string{"https://","http://"}
		urlSuffix := "/ads.txt"
		errCount := 0
		for i, urlPref := range urlPrefix {
			c := http.Client{Timeout: crawlerTimeout * time.Second}
			
			req, err := http.NewRequest("GET", urlPref + site.site + urlSuffix, nil)
			req.Header.Add("User-Agent", "Mediapartners-Google")
			req.Close = true
			resp, err := c.Do(req)
			if err != nil {
				if err, ok := err.(*url.Error); ok {
    				if err, ok := err.Err.(*net.OpError); ok {
        				if _, ok := err.Err.(*net.DNSError); ok {
            				res <- ReqRes{site, rrBadSrvName, nil}
							break
        				}
    				}
				}// otherwise it may be timeout
				errCount +=1
			} else {
				body, err := ioutil.ReadAll(resp.Body)
				if err != nil {
					//fmt.Print(err)
					i += 2 //status code will be 2xxx or 3xxx
				}
				resp.Body.Close()
				res <- ReqRes{site, i * 1000 + resp.StatusCode, body}
				break
			}
		}
		if errCount == 2 {
			res <- ReqRes{site, rrNeitherProtocolResponse, nil}
		}
	}
	
	if wg != nil {
		wg.Done()
	}
}

func loadSitesList(fname string, db * sql.DB, sites chan Site) {
	//reads file, adds to DB sites if not there already,
	//pass to channel with urls and ids
	f, err := os.Open(fname)
	defer f.Close()
	defer close(sites)
	if err != nil {
		fmt.Println(err)
		return
	}
	
	rdr := bufio.NewReader(f)
	
	for line, _, _ := rdr.ReadLine(); line != nil; line, _, _ = rdr.ReadLine(){
		var id int
		parts := strings.Split(string(line), ",")//parts[0] - position, parts[1] - site
		err := db.QueryRow("SELECT siteid FROM site WHERE siteaddr=$1",parts[1]).Scan(&id)
		if err != nil {//need to add
			var arank int
			arank, err = strconv.Atoi(parts[0])
			if err != nil { // failed to parse site rank
				fmt.Println(parts[0], err)
				continue
			}
			err = db.QueryRow("INSERT INTO site (arank,siteaddr) VALUES ($1,$2) RETURNING siteid",arank,parts[1]).Scan(&id)
		}
		
		if err != nil {// failed insert
			fmt.Println(err)
			return
		}
		
		sites <- Site{id, parts[1]}
	}
}

func updateAdVendors(pubs []Publisher, db * sql.DB, idsCache map[string]int) error {
	//sets advid field of Publisher
	
	for i, p := range pubs {
		var advid int
		if advid, alreadyCached := idsCache[p.advname]; alreadyCached {
			pubs[i].advid = advid
			continue
		}
		
		err := db.QueryRow("SELECT avid FROM advendor WHERE advendorname=$1",
						p.advname).Scan(&advid)
		if err != nil { // this AdVendor still not in DB, insert it into the TABLE
			err = db.QueryRow("INSERT INTO advendor (advendorname) VALUES ($1) RETURNING avid",
							p.advname).Scan(&advid)
			if err != nil { //insert failed - can not tolerate
				return err
			}

		}
		
		idsCache[p.advname] = advid
		pubs[i].advid = advid
	}
	
	return nil
}

func storeScanRes(sr chan ReqRes, db * sql.DB, done chan struct{}){
	//take result from channel sr
	//append record to scanresult TABLE
	//parse response
	//append records to advendor TABLE if necessary
	//delete old records from publishers TABLE
	//insert new records from ads.txt
	defer close(done)
	
	advids := make(map[string]int)//cache for vendor ids
	now := time.Now()
	fmt.Println("Started receiver",now.Format("2006-01-02 15:04:05"))
	time0 := now.Unix()
	countProceed := 0
	countUpdated := 0
	countPublishers := 0
	statusTicker := time.NewTicker(time.Second * 2)
	defer statusTicker.Stop()
	
	go func() {
		for {
			select {
			case <-done:
				return
			case t := <-statusTicker.C:
				spent := t.Unix() - time0
				fmt.Printf("\r%02d:%02d:%02d | Scanned %d (%.1f/s), updated %d rows for %d sites | adVendors: %d",
						spent / 3600, (spent % 3600) / 60, spent % 60, countProceed,
						float32(countProceed) / float32(spent), countPublishers, countUpdated, len(advids))
			}
		}
	}()
	
	for r := range sr {
		countProceed ++
		
		h := make([]byte, 16)
		if r.status >= 0 && len(r.respBody) != 0 {
			hasher := fnv.New128()
			hasher.Write(r.respBody)
			h = hasher.Sum(nil)
		}
		
		foundHash := make([]byte, 16)
		
		skipPublisherUpdate := true // assume no new ads.txt
		
		err := db.QueryRow("SELECT resphash FROM scanresult WHERE siteid=$1 ORDER BY scandt DESC LIMIT 1",
						r.site.siteid).Scan(&foundHash)
		
		if err != nil { // no previously stored results, everything is new to us
			skipPublisherUpdate = false
		} else if !bytes.Equal(foundHash, h) { // if hash is changed - delete obsolete records
			db.Exec("DELETE FROM publisher WHERE siteid=$1", r.site.siteid)
			skipPublisherUpdate = false
		}

		_, err = db.Exec("INSERT INTO scanresult (siteid,scandt,result,resphash) VALUES ($1,LOCALTIMESTAMP,$2,$3)",
						r.site.siteid, r.status, h)

		if err != nil {// failed insert
			fmt.Println(err)
			return
		}
		
		if skipPublisherUpdate {
			continue
		}
		
		countUpdated ++
		
		publishers := make([]Publisher,0,64)
		rdr := bytes.NewBuffer(r.respBody)
		for {
			line, err := rdr.ReadString('\n')
			if pal, ok := parseAdsLine(line); ok {
				isUniq := true
				for _, p := range publishers {
					if pal.advid == p.advid && pal.publisherid == p.publisherid {
						isUniq = false
						break
					}
				}
				
				if isUniq {
					publishers = append(publishers, pal)
				}
			}
			
			if err == io.EOF {
				break
			}
		}
		
		if len(publishers) == 0 {
			continue
		}
		
		countPublishers += len(publishers)
		
		err = updateAdVendors(publishers, db, advids)// lookup advid/insert advendors
		
		if err != nil { // it's because insert advendor failed, why?
			fmt.Println(err)
			return
		}
		
		valuesPlaceholders := make([]string, len(publishers))
    	insertVals := make([]interface{}, len(publishers) * 3)

    	for i, pu := range publishers {
        	valuesPlaceholders[i] = fmt.Sprintf("($%d, $%d, $%d)",
        							i * 3 + 1, i * 3 + 2, i * 3 + 3)
        	insertVals[i * 3 + 0] = pu.advid
        	insertVals[i * 3 + 1] = r.site.siteid
        	insertVals[i * 3 + 2] = pu.publisherid
    	}
    	insertQuery := fmt.Sprintf("INSERT INTO publisher (pavid, siteid, publisherid) VALUES %s",
    								strings.Join(valuesPlaceholders, ","))
    	_, err = db.Exec(insertQuery, insertVals...)
    	if err != nil {// failed insert
			fmt.Println(err)
			return
		}
	}
	fmt.Println("\nFinished receiver",time.Now().Format("2006-01-02 15:04:05"))
}

func main() {
	fname := ""
	crawlersCount := defaultCrawlersCount
	argsOffset := 0
    if os.Args[0] == "go" { // go run thisProgram filenameIsArg[3]
    	argsOffset = 2
    }
    
    if len(os.Args) > 1 + argsOffset {
    	fname = os.Args[1 + argsOffset]
    }

    if len(os.Args) > 2 + argsOffset {
    	cc, err := strconv.Atoi(os.Args[2 + argsOffset])
    	if err == nil {
    		crawlersCount = cc
    		if crawlersCount < minCrawlersCount {
    			crawlersCount = minCrawlersCount
    		}
    	}
    }
    
	src := make(chan Site, crawlersCount)
	res := make(chan ReqRes, crawlersCount)
	saved := make(chan struct{})  //channel to wait goroutine storeScanRes finished 

    if len(fname) != 0 {
    	
    	db, err := sql.Open("postgres", dbConnectionString)
    	if err != nil {
        	panic(err)
    	} 
    	defer db.Close()
    	
    	var wg sync.WaitGroup
    	
    	for i:=0; i < crawlersCount; i ++ {
    		wg.Add(1)
    		go getAds(src,res, &wg)
    	}

    	go loadSitesList(fname, db, src)
    	go storeScanRes(res, db, saved)
    	
    	wg.Wait()  // wait for all crawl results are fed into channel
    	close(res)
    	<-saved    // wait for all results saved to DB
    }else {//just show getAds is working

		go getAds(src, res, nil)
		src <- Site{0,"rbc.ru"}

		rr := <-res
		fmt.Println(string(rr.respBody))
	}
}

//select s.siteid,s.siteaddr from scanresult r right join site s on r.siteid=s.siteid where r.siteid is null order by siteid;
