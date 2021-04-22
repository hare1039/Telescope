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
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/oleiade/lane"
	"github.com/unki2aut/go-mpd"
)

var remote *url.URL
var ipfsCaches map[string]*IPFSCache
var backEndBandwidth float64 = 25 * 1024 * 1024
var deltaRate float64 = 0.6
var IPFSDelay uint64 = 0
var httpHeadRequests chan func()

type emptyT struct{}

var busyChan chan emptyT
var busyQueue *lane.Deque

func updateBackendBandwidth(curBW float64) {
	backEndBandwidth = deltaRate*backEndBandwidth + (1.0-deltaRate)*curBW
	//	backEndBandwidth = 25 * 1024 * 1024
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

func preloadNextSegment(ipfscache *IPFSCache, fullpath string) {
	ID, number := ipfscache.ParseIDNumber(fullpath)

	pathkey := path.Dir(fullpath)
	for prefix, value := range ipfscache.URLMatcher {
		if value.ID == ID {
			busyChan <- emptyT{}
			httpHeadRequests <- func() {
				next := "http://" + remote.Host + pathkey + "/" + prefix + fmt.Sprint(number+1) + value.Suffix

				log.Println("HTTP Get:", next)
				ipfscache.AddRecord(number, ID)

				startReq := time.Now()
				if resp, err := http.Get(next); err == nil {
					delta := time.Since(startReq)
					defer resp.Body.Close()

					currentBandwidthNS := float64(resp.ContentLength) / float64(delta/time.Nanosecond)
					curBW := currentBandwidthNS * float64(time.Second) * float64(time.Nanosecond)
					updateBackendBandwidth(curBW)
				} else {
					log.Println("HTTP HEAD failed:", err)
				}
			}
			break
		}
	}
}

func findCachedSegment(fullpath string) string {
	ID, number := ipfscache.ParseIDNumber(fullpath)

	pathkey := path.Dir(fullpath)
	for prefix, value := range ipfscache.URLMatcher {
		if value.ID == ID {
			return "http://" + remote.Host + pathkey + "/" + prefix + fmt.Sprint(number) + value.Suffix
		}
	}

	return "" // should not be here
}

func proxyHandle(c *gin.Context) {
	fullpath := c.Param("path")
	pathkey := path.Dir(fullpath)
	clientID := c.Request.Header.Get("clientID")
	frontBW := c.Request.Header.Get("frontBW")
	if clientID != "" {
		log.Println("clientID:", clientID)
		log.Println("clientBW:", frontBW)
	}

	IPFSCachedPath := false
	if ipfscache, ok := ipfsCaches[pathkey]; ok {
		IPFSCachedPath = ipfsCaches[pathkey].AlreadyCached(fullpath)
		ipfscache.AddRecordFromURL(clientID, fullpath)
	}

	proxy := httputil.NewSingleHostReverseProxy(remote)
	proxy.Director = func(req *http.Request) {
		req.Header = c.Request.Header
		delete(req.Header, "If-Modified-Since")
		delete(req.Header, "If-None-Match")
		req.Host = remote.Host
		req.URL.Scheme = remote.Scheme
		req.URL.Host = remote.Host
		req.URL.Path = fullpath
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
	go func() {
		if ipfscache, ok := ipfsCaches[pathkey]; ok {
			preloadNextSegment(ipfscache, fullpath)
		}
	}()
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
