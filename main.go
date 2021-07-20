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
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/unki2aut/go-mpd"
	"github.com/unki2aut/go-xsd-types"
)

var remote *url.URL
var ipfsCaches map[string]*IPFSCache
var deltaRate float64 = 0.50
var IPFSDelay uint64 = 0
var httpHeadRequests chan func()
var SetupMode bool

type ClientThroughput struct {
	Uncached float64
	Cached   float64
}

var clientThroughput map[string]ClientThroughput

func requestBackend() {
	for req := range httpHeadRequests {
		req()
	}
}

func preloadNextSegment(clientID string, ipfscache *IPFSCache, fullpath string) {
	if SetupMode {
		return
	}
	segment, quality := ipfscache.ParseSegmentQuality(fullpath)

	if segment == 0 {
		return
	}

	qualityList := make([]int, 0)
	for _, value := range ipfscache.URLMatcher {
		qualityList = append(qualityList, value.Quality)
	}

	sort.Sort(sort.Reverse(sort.IntSlice(qualityList)))

FindBestQuality:
	for _, highestQuality := range qualityList {
		for _, value := range ipfscache.URLMatcher {
			if value.Quality == highestQuality &&
				value.Bandwidth < clientThroughput[clientID].Cached {
				quality = value.Quality
				break FindBestQuality
			}
		}
	}

	nextsegment := segment + 1
	pathkey := path.Dir(fullpath)
	next := remote.Scheme + "://" + remote.Host + pathkey + "/" + ipfscache.FormUrlBySegmentQuality(nextsegment, quality)

	httpHeadRequests <- func() {
		log.Println("Prefetch Segment", segment, "Quality", quality)
		if resp, err := http.Get(next); err == nil {
			ioutil.ReadAll(resp.Body)
			defer resp.Body.Close()
			ipfscache.AddRecord(nextsegment, quality, clientID)
		}
	}
}

func proxyHandle(c *gin.Context) {
	fullpath := c.Param("path")
	pathkey := path.Dir(fullpath)
	pathname := filepath.Base(fullpath)
	clientID := c.Request.Header.Get("clientID")
	log.Println("Processing request", pathname)

	if _, ok := clientThroughput[clientID]; !ok {
		clientThroughput[clientID] = ClientThroughput{
			Cached:   15.0 * 1000 * 1000,
			Uncached: 10.0 * 1000 * 1000,
		}
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

	if strings.Contains(pathname, ".mpd") {
		proxy.ModifyResponse = func(r *http.Response) error {
			if r.StatusCode != 200 {
				log.Println(r)
			}

			b, err := ioutil.ReadAll(r.Body)
			if err != nil {
				panic(err)
			}

			mpdv := new(mpd.MPD)
			if err := mpdv.Decode(b); err != nil {
				log.Println("mpd decode error", err)
				log.Println(r.Body)
				return err
			}

			if _, ok := ipfsCaches[pathkey]; !ok {
				ipfsCaches[pathkey] = NewIPFSCache(mpdv)
			}

			mpdtype := "dynamic"
			mpdv.Type = &mpdtype
			mpdv.MinimumUpdatePeriod = &xsd.Duration{Seconds: 1}

			timeZero, _ := xsd.DateTimeFromString("1970-01-01T00:00:00Z")
			mpdv.AvailabilityStartTime = timeZero

			ipfscache := ipfsCaches[pathkey]
			//			clientVideoBandwidth := ipfscache.QualitysBandwidth(ipfscache.PrevReqQuality[clientID])
			cachedSet, latest := ipfscache.Latest(clientID)
			log.Println("For segment", latest, ":", cachedSet)

			var off uint64 = 0
			for _, p := range mpdv.Period {
				for _, adapt := range p.AdaptationSets {
					for i, _ := range adapt.Representations {
						representation := &adapt.Representations[i]
						representation.SegmentTemplate.PresentationTimeOffset = &off

						if !cachedSet.Has(Stoi(*representation.ID)) {
							// DownloadTime / MPD_BW = AbrLimitTime / NEW_BW
							size := float64(*representation.SegmentTemplate.Duration) * float64(*representation.Bandwidth)
							rate := (size / clientThroughput[clientID].Uncached) / (size / clientThroughput[clientID].Cached)

							log.Println("Rewrite bw with rate", rate)
							if rate < 1.0 {
								log.Println("skip larger rewrite")
							} else {
								*representation.Bandwidth = uint64(float64(*representation.Bandwidth) * rate)
							}
						}
					}
				}
			}
			mpdv.Period[0].Start = &xsd.Duration{}

			newmpd, err := mpdv.Encode()
			if err != nil {
				fmt.Println("Encode failed. Returning the original one:", err)
				return nil
			}

			buf := bytes.NewBuffer(newmpd)
			r.Body = ioutil.NopCloser(buf)
			r.Header["Content-Length"] = []string{fmt.Sprint(buf.Len())}
			r.Header["Last-Modified"] = []string{time.Now().UTC().Format(http.TimeFormat)}
			r.Header["Cache-Control"] = []string{"no-cache"}
			//			log.Println("MPD Modified")
			return nil
		}
	}

	defer func() {
		if r := recover(); r != nil {
			log.Println("Recover from", r)
		}
	}()

	var t time.Time

	transferDone := make(chan struct{})
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Println(pathname, "handler recovery", r)
			}
		}()

		t = time.Now()
		proxy.ServeHTTP(c.Writer, c.Request)
		transferDone <- struct{}{}
	}()

	requestTimeout := 15 * time.Second
	if SetupMode {
		requestTimeout = 2 * 60 * time.Second
	}
	select {
	case <-transferDone:
		close(transferDone)
	case <-time.After(requestTimeout):
		log.Println(pathname, "trying close")
		c.Request.Body.Close()
	}

	c.Writer.Flush()
	transferTime := time.Since(t)
	if ipfscache, ok := ipfsCaches[pathkey]; ok && c.Writer.Size() > 400000 {
		preloadNextSegment(clientID, ipfscache, fullpath)
		isCached := ipfscache.AlreadyCachedUrl(fullpath)

		currentBandwidthNS := float64(c.Writer.Size()*8) / float64(transferTime.Nanoseconds())
		curBW := currentBandwidthNS * float64(time.Second) * float64(time.Nanosecond)

		var ct = clientThroughput[clientID]
		if isCached {
			ct.Cached = deltaRate*ct.Cached + (1.0-deltaRate)*curBW
			log.Println("Update cachedThroughput", int64(ct.Cached/1000), "kbits")
		} else {
			ct.Uncached = deltaRate*ct.Uncached + (1.0-deltaRate)*curBW
			log.Println("Update uncachedThroughout", int64(ct.Uncached/1000), "kbits")
		}
		clientThroughput[clientID] = ct
		ipfscache.AddRecordFromURL(fullpath, clientID)
	}
}

func settings(c *gin.Context) {
	if c.PostForm("timeout") == "1" {
		SetupMode = true
	} else {
		SetupMode = false
	}
	log.Println("set SetupMode to", SetupMode)
}

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
	clientThroughput = make(map[string]ClientThroughput)
	SetupMode = false

	go requestBackend()

	r := gin.Default()
	//	r.Use(func(c *gin.Context) {
	//		FrontendBandwidthEstimate(c)
	//	})

	r.GET("/*path", proxyHandle)
	r.POST("/settings", settings)
	//	r.Any("/*path", pureProxyHandle)

	s := http.Server{
		Addr:    os.Args[2],
		Handler: r,
	}
	s.SetKeepAlivesEnabled(false)
	s.ListenAndServe()
}
