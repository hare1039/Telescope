package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/oleiade/lane"
	"github.com/unki2aut/go-mpd"
)

var remote *url.URL
var ipfsCaches map[string]*IPFSCache
var backEndBandwidth float64 = 16 * 1000 * 1000
var deltaRate float64 = 0.6
var IPFSDelay uint64 = 0
var clientTrace map[string][]string
var httpHeadRequests chan func()

type emptyT struct{}

var busyChan chan emptyT
var busyQueue *lane.Deque

func Stoi(s string) int {
	r, _ := strconv.Atoi(s)
	return r
}

func updateBackendBandwidth(curBW float64) {
	backEndBandwidth = deltaRate*backEndBandwidth + (1.0-deltaRate)*curBW
	//	backEndBandwidth = 16 * 1000 * 1000
	log.Println("Update backEndBandwidth", int64(backEndBandwidth/1024), "kbits")
}

func httpHeadRequest() {
	for exe := range httpHeadRequests {
		select {
		case <-busyChan:
			exe()
		}

		if busyQueue.Empty() {
			busyChan <- emptyT{}
		} else {
			for len(busyChan) > 0 {
				<-busyChan
			}
		}
	}
}

func preloadNextSegment(clientBW float64, ipfscache *IPFSCache, fullpath string) {
	_, number := ipfscache.ParseIDNumber(fullpath)

	bandwidth := clientBW
	if backEndBandwidth < bandwidth {
		bandwidth = backEndBandwidth
	}
	log.Println("limit:", int64(bandwidth/1024), "kbits")

	idList := make([]int, 0)
	for _, value := range ipfscache.URLMatcher {
		idList = append(idList, Stoi(value.ID))
	}

	sort.Sort(sort.Reverse(sort.IntSlice(idList)))
	pathkey := path.Dir(fullpath)

	for _, highestID := range idList {
		for prefix, value := range ipfscache.URLMatcher {
			if value.Bandwidth < bandwidth && value.ID == fmt.Sprint(highestID) {
				busyChan <- emptyT{}
				httpHeadRequests <- func() {
					url := pathkey + "/" + prefix + fmt.Sprint(number+1) + value.Suffix
					if ipfscache.AlreadyCached(url) {
						log.Println("Already cached, skip", url)
						return
					}

					next := remote.Scheme + "://" + remote.Host + url

					log.Println("HTTP Get:", next)
					ipfscache.AddRecord(number+1, fmt.Sprint(highestID))

					startReq := time.Now()
					if resp, err := http.Get(next); err == nil {
						delta := time.Since(startReq)
						defer resp.Body.Close()

						currentBandwidthNS := float64(resp.ContentLength*8) / float64(delta/time.Nanosecond)
						curBW := currentBandwidthNS * float64(time.Second) * float64(time.Nanosecond)
						updateBackendBandwidth(curBW)
					} else {
						log.Println("HTTP HEAD failed:", err)
					}
				}
				return
			}
		}

	}
}

func findCachedSegment(clientBW float64, ipfscache *IPFSCache, fullpath string) (string, string) {
	ID, number := ipfscache.ParseIDNumber(fullpath)

	if ipfscache.IPFSCachedSegments[number] == nil {
		return fullpath, ID
	}

	bandwidth := clientBW
	if backEndBandwidth < bandwidth {
		bandwidth = backEndBandwidth
	}

	//	log.Println("saved quality", ipfscache.IPFSCachedSegments[number])
	pathkey := path.Dir(fullpath)

	idList := make([]int, 0)
	for _, value := range ipfscache.IPFSCachedSegments[number].List() {
		idList = append(idList, Stoi(value))
	}

	sort.Sort(sort.Reverse(sort.IntSlice(idList)))
	//	log.Println(idList)
	//	log.Println(ipfscache.IPFSCachedSegments[number])
	for _, ID := range idList {
		log.Println("Avalble", ID)
		for prefix, value := range ipfscache.URLMatcher {
			if fmt.Sprint(ID) == value.ID && value.Bandwidth < bandwidth {

				return pathkey + "/" + prefix + fmt.Sprint(number) + value.Suffix, value.ID
			}
		}
	}

	return fullpath, ID
}

func proxyHandle(c *gin.Context) {
	fullpath := c.Param("path")
	pathkey := path.Dir(fullpath)
	clientID := c.Request.Header.Get("clientID")
	frontBW, _ := strconv.ParseFloat(c.Request.Header.Get("frontBW"), 64)
	//	if clientID != "" {
	//		log.Println("clientID:", clientID)
	//		log.Println("clientBW:", frontBW)
	//	}

	IPFSCachedPath := false
	editedpath := fullpath
	var segmentID string
	if ipfscache, ok := ipfsCaches[pathkey]; ok {
		editedpath, segmentID = findCachedSegment(frontBW, ipfscache, fullpath)
		//		log.Println("segmentID", segmentID)
		IPFSCachedPath = ipfsCaches[pathkey].AlreadyCached(fullpath)
		//		ipfscache.AddRecordFromURL(clientID, fullpath)
		go preloadNextSegment(frontBW, ipfscache, fullpath)
	}
	if fullpath == editedpath {
		log.Println("No change")
	} else {
		log.Println("=>", editedpath)
	}

	if strings.Contains(fullpath, "api-special") {
		c.JSON(200, clientTrace[clientID])
		return
	}

	proxy := httputil.NewSingleHostReverseProxy(remote)
	proxy.Director = func(req *http.Request) {
		req.Header = c.Request.Header
		delete(req.Header, "If-Modified-Since")
		delete(req.Header, "If-None-Match")
		req.Host = remote.Host
		req.URL.Scheme = remote.Scheme
		req.URL.Host = remote.Host
		req.URL.Path = editedpath
		if segmentID != "" {
			clientTrace[clientID] = append(clientTrace[clientID], segmentID)
		}
	}

	if strings.Contains(fullpath, ".mpd") {
		proxy.ModifyResponse = func(r *http.Response) error {
			if r.StatusCode != 200 {
				log.Println(r)
			}

			b, err := ioutil.ReadAll(r.Body)
			if err != nil {
				panic(err)
			}

			mpd := new(mpd.MPD)
			if err := mpd.Decode(b); err != nil {
				log.Println("mpd decode error", err)
				log.Println(r.Body)
				return err
			}

			if _, ok := ipfsCaches[pathkey]; !ok {
				ipfsCaches[pathkey] = NewIPFSCache(mpd)
			}

			buf := bytes.NewBuffer(b)
			r.Body = ioutil.NopCloser(buf)
			r.Header["Content-Length"] = []string{fmt.Sprint(buf.Len())}
			r.Header["Last-Modified"] = []string{time.Now().UTC().Format(http.TimeFormat)}
			r.Header["Cache-Control"] = []string{"no-cache"}

			return nil
		}
	} else {
		busyQueue.Append(emptyT{})

		defer func() {
			busyQueue.Pop()
			if busyQueue.Empty() && IPFSCachedPath {
				busyChan <- emptyT{}
			}
		}()
	}

	defer func() {
		if r := recover(); r != nil {
			log.Println("Recover from", r)
		}
	}()

	proxy.ServeHTTP(c.Writer, c.Request)
}

//func FrontendBandwidthEstimate(c *gin.Context) {
//	t := time.Now()
//	log.Println("before")
//
//	c.Next()
//
//	delta := time.Since(t)
//	log.Println("after", delta, c.Writer.Status())
//}

func main() {
	if len(os.Args) != 3 {
		fmt.Println(os.Args[0] + " ipfs_gateway listen_address")
	}

	var err error
	remote, err = url.Parse(os.Args[1])
	if err != nil {
		panic(err)
	}

	ipfsCaches = make(map[string]*IPFSCache)
	httpHeadRequests = make(chan func(), 1000)
	clientTrace = make(map[string][]string)
	busyChan = make(chan emptyT, 100000)
	busyQueue = lane.NewDeque()
	go httpHeadRequest()

	r := gin.Default()
	//	r.Use(func(c *gin.Context) {
	//		FrontendBandwidthEstimate(c)
	//	})

	r.Any("/*path", proxyHandle)
	//	r.Any("/*path", pureProxyHandle)

	r.Run(os.Args[2])
}
