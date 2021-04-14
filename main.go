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
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/oleiade/lane"
	"github.com/unki2aut/go-mpd"
	"github.com/unki2aut/go-xsd-types"
)

var remote *url.URL
var ipfsCaches map[string]*IPFSCache
var backEndBandwidth uint64 = 0
var deltaRate float64 = 0.6
var IPFSDelay uint64 = 0
var httpHeadRequests chan func()

type emptyT struct{}

var busyChan chan emptyT
var busyQueue *lane.Deque

func httpHeadRequest() {
	for exe := range httpHeadRequests {
		select {
		case <-busyChan:
			exe()
		}

		if busyQueue.Empty() {
			busyChan <- emptyT{}
		}
	}
}

func preloadHigherBitrate(ipfscache *IPFSCache, fullpath string) {
	ID, number := ipfscache.ParseIDNumber(fullpath)
	IDnum, _ := strconv.Atoi(ID)
	for {
		IDnum += 1
		ID = fmt.Sprint(IDnum)
		if ipfscache.IPFSCachedSegments[number] == nil ||
			!ipfscache.IPFSCachedSegments[number].Has(ID) {
			break
		}
	}

	pathkey := path.Dir(fullpath)
	for prefix, value := range ipfscache.URLMatcher {
		if value.ID == ID {
			httpHeadRequests <- func() {
				next := "http://" + remote.Host + pathkey + "/" + prefix + fmt.Sprint(number) + value.Suffix
				//log.Println("HTTP HEAD:", next)
				//if _, err := http.Head(next); err == nil {
				log.Println("HTTP Get:", next)
				if _, err := http.Get(next); err == nil {
					ipfscache.AddRecord(number, ID)
				} else {
					log.Println("HTTP HEAD failed:", err)
				}
			}
			break
		}
	}
}

func proxyHandle(c *gin.Context) {
	fullpath := c.Param("path")
	pathkey := path.Dir(fullpath)
	clientID := c.Request.Header.Get("clientID")
	log.Println("clientID:", clientID)

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

	enableModify := true
	IPFSCachedPath := true
	if ipfscache, ok := ipfsCaches[pathkey]; ok {
		IPFSCachedPath = ipfsCaches[pathkey].AlreadyCached(fullpath)

		err := ipfscache.AddRecordFromURL(clientID, fullpath)
		if err != nil || ipfscache.ShouldEnd[clientID] {
			enableModify = false
			log.Println("enableModify off")
		}
	}

	if strings.Contains(fullpath, ".mpd") && enableModify {
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

			mpdtype := "dynamic"
			mpd.Type = &mpdtype
			mpd.MinimumUpdatePeriod = &xsd.Duration{Seconds: 1}

			timeZero, _ := xsd.DateTimeFromString("1970-01-01T00:00:00Z")
			mpd.AvailabilityStartTime = timeZero

			cachedSet, latest := ipfsCaches[pathkey].Latest(clientID)
			log.Println("For segment", latest, ":", cachedSet)

			if IPFSDelay == 0 {
				for _, p := range mpd.Period {
					for _, adapt := range p.AdaptationSets {
						for _, representation := range adapt.Representations {
							if *representation.Bandwidth > IPFSDelay {
								IPFSDelay = *representation.Bandwidth
								log.Println(*representation.Bandwidth)
							}
						}
					}
				}
			}

			var more uint64 = 0
			if IPFSDelay > backEndBandwidth {
				more = IPFSDelay - backEndBandwidth
			}

			var off uint64 = 0
			for _, p := range mpd.Period {
				for _, adapt := range p.AdaptationSets {
					writeBandwidth := true
					for i, _ := range adapt.Representations {
						representation := &adapt.Representations[i]
						representation.SegmentTemplate.PresentationTimeOffset = &off

						if cachedSet.IsEmpty() {
							continue
						}

						if writeBandwidth {
							if !cachedSet.Has(*representation.ID) {
								*representation.Bandwidth += more
							} else {
								log.Println("Skip bandwidth after", *representation.ID, "by", more)
								writeBandwidth = false
							}
						}
					}
				}
			}
			mpd.Period[0].Start = &xsd.Duration{}

			newmpd, err := mpd.Encode()
			if err != nil {
				fmt.Println("Encode failed. Returning the original one:", err)
				return nil
			}

			buf := bytes.NewBuffer(newmpd)
			r.Body = ioutil.NopCloser(buf)
			r.Header["Content-Length"] = []string{fmt.Sprint(buf.Len())}
			r.Header["Last-Modified"] = []string{time.Now().UTC().Format(http.TimeFormat)}
			r.Header["Cache-Control"] = []string{"no-cache"}
			log.Println("MPD Modified")
			return nil
		}
	} else {
		busyQueue.Append(emptyT{})

		defer func() {
			busyQueue.Pop()
			if busyQueue.Empty() && (!enableModify || IPFSCachedPath) {
				busyChan <- emptyT{}
			}
		}()
	}

	defer func() {
		if r := recover(); r != nil {
			log.Println("Recover from", r)
		}
	}()

	startReq := time.Now().UnixNano()
	proxy.ServeHTTP(c.Writer, c.Request)
	delta := time.Now().UnixNano() - startReq

	if c.Writer.Size() > 10000 && delta != 0 && !IPFSCachedPath {
		currentBandwidthNS := float64(c.Writer.Size()) / float64(delta)
		curBW := currentBandwidthNS * float64(time.Second) * float64(time.Nanosecond)

		backEndBandwidth = uint64(deltaRate*float64(backEndBandwidth) + (1.0-deltaRate)*curBW)
		log.Println("Update backEndBandwidth", backEndBandwidth)

		if ipfscache, ok := ipfsCaches[pathkey]; ok {
			go preloadHigherBitrate(ipfscache, fullpath)
		}
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
	httpHeadRequests = make(chan func(), 1000)
	busyChan = make(chan emptyT, 100000)
	busyQueue = lane.NewDeque()
	go httpHeadRequest()

	r := gin.Default()

	r.Any("/*path", proxyHandle)

	r.Run(os.Args[2])
}
