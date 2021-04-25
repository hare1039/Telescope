package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"net"
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
var deltaRate float64 = 0.75
var IPFSDelay uint64 = 0
var clientTrace map[string][]int
var clientLatestTransmit map[string]time.Duration
var httpHeadRequests chan func()

type emptyT struct{}

var busyChan chan emptyT
var busyQueue *lane.Deque

func requestBackend() {
	for req := range httpHeadRequests {
		req()
	}
}

func updateBackendBandwidth(curBW float64) {
	backEndBandwidth = deltaRate*backEndBandwidth + (1.0-deltaRate)*curBW
	//	backEndBandwidth = 16 * 1000 * 1000
	log.Println("Update backEndBandwidth", int64(backEndBandwidth/1000), "kbits")
	//log.Println("Current BW", int64(curBW/1000), "kbits")
}

func findNextSegment(
	clientBW float64,
	ipfscache *IPFSCache,
	segment uint64,
	requestedQuality int,
	backingOff bool) (uint64, int) {

	bandwidth := clientBW // = min(clientBW, backEndBandwidthx)
	if backEndBandwidth < bandwidth {
		bandwidth = backEndBandwidth
	}
	log.Println(int64(clientBW/1000), int64(backEndBandwidth/1000), "kbits")

	advanceQuality := requestedQuality
	if !backingOff {
		greatestQuality := ipfscache.GreatestQuality(segment)
		if advanceQuality > greatestQuality {
			advanceQuality = greatestQuality + 1
		}
	}

	qualityList := make([]int, 0)
	for _, value := range ipfscache.URLMatcher {
		qualityList = append(qualityList, value.Quality)
	}

	sort.Sort(sort.Reverse(sort.IntSlice(qualityList)))

	for _, highestQuality := range qualityList {
		for _, value := range ipfscache.URLMatcher {
			if value.Quality == highestQuality &&
				((value.Bandwidth < bandwidth && !backingOff) || value.Quality == advanceQuality) {
				if ipfscache.AlreadyCached(segment, value.Quality) {
					log.Println("Already cached, skip", value.Quality)
					return findNextSegment(clientBW, ipfscache, segment+1, requestedQuality, false)
				}

				return segment, value.Quality
			}
		}
	}
	return segment, requestedQuality
}

func preloadNextSegment(clientID string, clientBW float64, ipfscache *IPFSCache, fullpath string) {
	segment, quality := ipfscache.ParseSegmentQuality(fullpath)

	backoff := false
	if clientLatestTransmit[clientID] > ipfscache.SegmentDuration {
		log.Println("Ohno too slow, backing off")
		backoff = true
		//		quality = clientTrace[clientID][len(clientTrace[clientID])-1] - 1
	}

	targetSegment, targetQuality := findNextSegment(clientBW, ipfscache, segment+1, quality, backoff)

	pathkey := path.Dir(fullpath)
	next := remote.Scheme + "://" + remote.Host + pathkey + "/" + ipfscache.FormUrlBySegmentQuality(targetSegment, targetQuality)

	log.Println("IPFS Get Segment", targetSegment, "Quality", targetQuality)
	httpHeadRequests <- func() {
		startReq := time.Now()
		if resp, err := http.Get(next); err == nil {
			delta := time.Since(startReq)
			ipfscache.AddRecord(targetSegment, targetQuality)
			defer resp.Body.Close()

			if delta.Milliseconds() > 100 {
				currentBandwidthNS := float64(resp.ContentLength*8) / float64(delta.Nanoseconds())
				curBW := currentBandwidthNS * float64(time.Second) * float64(time.Nanosecond)
				updateBackendBandwidth(curBW)
			}

			if delta > ipfscache.SegmentDuration {
				log.Println("poping hold requests")
				for len(httpHeadRequests) > 0 {
					<-httpHeadRequests
				}
				log.Println("poping hold requests done")
			}
		}
	}
}

func findCachedQuality(clientBW float64, ipfscache *IPFSCache, fullpath string) (string, int) {
	segment, quality := ipfscache.ParseSegmentQuality(fullpath)

	if ipfscache.IPFSCachedSegments[segment] == nil {
		log.Println("Running late", segment)
		return fullpath, quality
	}

	log.Println("Requested quality", quality)
	pathkey := path.Dir(fullpath)

	qualityList := make([]int, 0)
	for _, value := range ipfscache.IPFSCachedSegments[segment].List() {
		qualityList = append(qualityList, value)
	}

	sort.Sort(sort.Reverse(sort.IntSlice(qualityList)))
	log.Println(segment, "Available:", qualityList)

	for _, bestQuality := range qualityList {
		for prefix, value := range ipfscache.URLMatcher {
			if bestQuality == value.Quality &&
				(value.Bandwidth < clientBW) {
				return pathkey + "/" + prefix + fmt.Sprint(segment) + value.Suffix, value.Quality
			}
		}
	}

	return fullpath, quality
}

func proxyHandle(c *gin.Context) {
	fullpath := c.Param("path")
	pathkey := path.Dir(fullpath)
	clientID := c.Request.Header.Get("clientID")
	frontBW, _ := strconv.ParseFloat(c.Request.Header.Get("frontBW"), 64)

	editedpath := fullpath

	var quality int = -1

	if ipfscache, ok := ipfsCaches[pathkey]; ok {
		preloadNextSegment(clientID, frontBW, ipfscache, fullpath)
		editedpath, quality = findCachedQuality(frontBW, ipfscache, fullpath)
	}

	if fullpath != editedpath {
		log.Println("=>", quality)
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

		if quality != -1 {
			clientTrace[clientID] = append(clientTrace[clientID], quality)
		}
		if ipfscache, ok := ipfsCaches[pathkey]; ok {
			ipfscache.AddRecordFromURL(editedpath)
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
	}

	defer func() {
		if r := recover(); r != nil {
			log.Println("Recover from", r)
		}
	}()

	t := time.Now()
	proxy.ServeHTTP(c.Writer, c.Request)
	clientLatestTransmit[clientID] = time.Since(t)
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
	clientTrace = make(map[string][]int)
	clientLatestTransmit = make(map[string]time.Duration)
	//	busyChan = make(chan emptyT, 100000)
	// busyQueue = lane.NewDeque()

	httpHeadRequests <- func() {
		for {
			u, _ := time.ParseDuration("30s")
			conn, _ := net.DialTimeout("tcp", "localhost:8080", u)
			if conn != nil {
				conn.Close()
				break
			}
		}
		log.Println("start measure back bw")

		startReq := time.Now()
		if resp, err := http.Get("http://localhost:8080/ipfs/bafybeidzblnh3666bize6k7lyut3gsjynxc5w25kst3444nelc5rwy74ge/1min_4k_dash/encoded_video/video_2500kbps.mp4"); err == nil {
			delta := time.Since(startReq)
			defer resp.Body.Close()

			currentBandwidthNS := float64(resp.ContentLength*8) / float64(delta/time.Nanosecond)
			curBW := currentBandwidthNS * float64(time.Second) * float64(time.Nanosecond)
			backEndBandwidth = curBW
			log.Println("Update backEndBandwidth", int64(backEndBandwidth/1000), "kbits")
		} else {
			log.Println("HTTP HEAD failed:", err)
		}
	}

	go requestBackend()

	r := gin.Default()
	//	r.Use(func(c *gin.Context) {
	//		FrontendBandwidthEstimate(c)
	//	})

	r.Any("/*path", proxyHandle)
	//	r.Any("/*path", pureProxyHandle)

	r.Run(os.Args[2])
}
