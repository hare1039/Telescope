package main

import (
	"bytes"
	_ "encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/hare1039/go-mpd"
	"github.com/unki2aut/go-xsd-types"

	"github.com/mikioh/tcp"
	"github.com/mikioh/tcpinfo"
)

var remote *url.URL
var ipfsCaches map[string]*IPFSCache
var deltaRate float64 = 0.50
var IPFSDelay uint64 = 0
var SetupMode bool
var MPDMainPolicy string
var PrefetchOff bool
var requestHighQuality bool

type ClientThroughput struct {
	Uncached  float64
	Cached    float64
	CurBW     float64
	CacheHist []float64
}

var clientThroughput map[string]ClientThroughput

func waitTransferEnd(c *gin.Context) {
	con, _, hijerr := c.Writer.Hijack()
	if hijerr != nil {
		return
	}
	tc, err := tcp.NewConn(con)
	if err != nil {
		return
	}

	var info tcpinfo.Info
	var b [256]byte

	if err != nil {
		return
	}

	s := "syn"
	for strings.Contains(s, "syn") ||
		strings.Contains(s, "established") {
		i, err := tc.Option(info.Level(), info.Name(), b[:])
		if err != nil {
			return
		}
		s = fmt.Sprintf("%v", i)
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
			mpdv.MinimumUpdatePeriod = &xsd.Duration{Seconds: 5}

			timeZero, _ := xsd.DateTimeFromString("1970-01-01T00:00:00Z")
			mpdv.AvailabilityStartTime = timeZero

			ipfscache := ipfsCaches[pathkey]
			cachedSet, latest := ipfscache.Latest(clientID)

			MPDPolicy := MPDMainPolicy
			log.Println("For segment", latest, ":", cachedSet)
			log.Println("REQRITINGUNIFORM", MPDPolicy)

			var off uint64 = 0
			for _, p := range mpdv.Period {
				for _, adapt := range p.AdaptationSets {
					for i, _ := range adapt.Representations {
						representation := &adapt.Representations[i]
						representation.SegmentTemplate.PresentationTimeOffset = &off

						duration := float64(*representation.SegmentTemplate.Duration / *representation.SegmentTemplate.Timescale)
						size := duration * float64(*representation.Bandwidth)
						// DownloadTime / MPD_BW = AbrLimitTime / NEW_BW

						if MPDPolicy == "CACHEBASED" {
							rate := (size / clientThroughput[clientID].Uncached) / duration

							if cachedSet.Has(Stoi(*representation.ID)) {
								if rate < 1.0 {
									log.Println("skip smaller rewrite", rate)
								} else {
									log.Println("Rewrite bw with rate", rate)
									*representation.Bandwidth = uint64(float64(*representation.Bandwidth) * rate)
								}
							}
						} else if MPDPolicy == "UNCACHEBASED" {
							rate := (size / clientThroughput[clientID].Cached) / duration

							if cachedSet.Has(Stoi(*representation.ID)) {
								if rate > 1.0 {
									log.Println("skip greater rewrite", rate)
								} else {
									log.Println("Rewrite bw with rate", rate)
									*representation.Bandwidth = uint64(float64(*representation.Bandwidth) * rate)
								}
							}
						} else if MPDPolicy == "UNIFORM" {

							var rate float64
							var cacstr string
							client := clientThroughput[clientID]
							cachehit := 0.0
							counttotal := float64(len(client.CacheHist))
							for _, v := range client.CacheHist {
								cachehit += v
							}
							cachemiss := counttotal - cachehit

							if cachedSet.Has(Stoi(*representation.ID)) {
								//fmt.Printf("C %12d\n", uint64(size/clientThroughput[clientID].Cached))
								cacstr = "cached"
								rate = (size / client.Cached) / duration
								if len(client.CacheHist) > 0 {
									rate = 1 + (rate-1)*(cachemiss/counttotal)
								}
							} else {
								//fmt.Printf("U %12d\n", uint64(size/clientThroughput[clientID].Uncached))
								cacstr = "UNCACH"
								rate = (size / client.Uncached) / duration
								if len(client.CacheHist) > 0 {
									rate = 1 + (rate-1)*(cachehit/counttotal)
								}
							}
							log.Println("Rewrite", cacstr, "bw with rate", rate)
							*representation.Bandwidth = uint64(float64(*representation.Bandwidth) * rate)
						} else if MPDPolicy == "UNCHANGE" || MPDPolicy == "BASELINE" {
							// skip rewrite for baseline
						} else {
							log.Println("ERROR!!!RRRRRRRRRR: Unknown policy:", MPDPolicy)
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
		requestTimeout = 60 * time.Second
	}

	aborted := false
	select {
	case <-transferDone:
		close(transferDone)
	case <-time.After(requestTimeout):
		log.Println(pathname, "trying close")
		//		c.Request.Body.Close()
		aborted = true
	}

	c.Writer.Flush()

	if !aborted {
		waitTransferEnd(c)
	}

	c.Request.Body.Close()

	transferTime := time.Since(t)
	if ipfscache, ok := ipfsCaches[pathkey]; ok && !aborted {
		isCached := ipfscache.AlreadyCachedUrl(fullpath)

		currentBandwidthNS := float64(c.Writer.Size()*8) / float64(transferTime.Nanoseconds())
		curBW := currentBandwidthNS * float64(time.Second) / float64(time.Nanosecond)

		var ct = clientThroughput[clientID]
		requestHighQuality = math.Abs(curBW-ct.Cached) < math.Abs(curBW-ct.Uncached)

		if isCached {
			ct.Cached = deltaRate*ct.Cached + (1.0-deltaRate)*curBW
			ct.CacheHist = append(ct.CacheHist, 1.0)
			log.Println("Update cachedThroughput", int64(ct.Cached/1000), "kbits")
		} else {
			ct.Uncached = deltaRate*ct.Uncached + (1.0-deltaRate)*curBW
			ct.CacheHist = append(ct.CacheHist, 0.0)
			log.Println("Update uncachedThroughout", int64(ct.Uncached/1000), "kbits")
		}
		ct.CurBW = curBW*deltaRate + ct.CurBW*(1.0-deltaRate)

		clientThroughput[clientID] = ct
		ipfscache.AddRecordFromURL(fullpath, clientID)
	}
}

func settings(c *gin.Context) {
	if t := c.PostForm("setup"); t != "" {
		SetupMode = t == "1"
		log.Println("set SetupMode to", SetupMode)
	}
	if p := c.PostForm("policy"); p != "" {
		MPDMainPolicy = p
		log.Println("set MPDMainPolicy to", MPDMainPolicy)
	}
	if pf := c.PostForm("prefetch"); pf != "" {
		PrefetchOff = pf == "0"
		log.Println("set PrefetchOff to", PrefetchOff)
	}
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
	clientThroughput = make(map[string]ClientThroughput)
	SetupMode = false
	MPDMainPolicy = "BASELINE"

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
