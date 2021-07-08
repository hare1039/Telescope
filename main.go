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
	"github.com/unki2aut/go-xsd-types"
)

var remote *url.URL
var ipfsCaches map[string]*IPFSCache
var uncachedThroughput float64 = 16 * 1000 * 1000
var deltaRate float64 = 0.50
var IPFSDelay uint64 = 0
var clientLatestTransmit map[string]time.Duration
var clientBandwidth map[string]float64
var httpHeadRequests chan func()

type emptyT struct{}

var busyChan chan emptyT
var busyQueue *lane.Deque

func requestBackend() {
	for req := range httpHeadRequests {
		req()
	}
}

func updateUncachedThroughput(curBW float64) {
	uncachedThroughput = deltaRate*uncachedThroughput + (1.0-deltaRate)*curBW
	log.Println("Update backEndBandwidth", int64(uncachedThroughput/1000), "kbits")
}

func preloadNextSegment(clientID string, ipfscache *IPFSCache, fullpath string) {
	segment, quality := ipfscache.ParseSegmentQuality(fullpath)

	if segment == 0 {
		return
	}

	nextsegment := segment + 1
	pathkey := path.Dir(fullpath)
	next := remote.Scheme + "://" + remote.Host + pathkey + "/" + ipfscache.FormUrlBySegmentQuality(nextsegment, quality)

	log.Println("IPFS Get Segment", segment, "Quality", quality)
	httpHeadRequests <- func() {
		startReq := time.Now()
		if resp, err := http.Get(next); err == nil {
			ioutil.ReadAll(resp.Body)
			delta := time.Since(startReq)
			defer resp.Body.Close()
			ipfscache.AddRecord(nextsegment, quality, clientID)

			if delta.Milliseconds() > 800 {
				currentBandwidthNS := float64(resp.ContentLength*8) / float64(delta.Nanoseconds())
				curBW := currentBandwidthNS * float64(time.Second) * float64(time.Nanosecond)
				updateUncachedThroughput(curBW)
			}
		}
	}
}

func proxyHandle(c *gin.Context) {
	fullpath := c.Param("path")
	pathkey := path.Dir(fullpath)
	clientID := c.Request.Header.Get("clientID")
	log.Println("Processing request", fullpath)

	if _, fOK := clientBandwidth[clientID]; !fOK {
		clientBandwidth[clientID] = 10 * 1000 * 1000
	}

	if ipfscache, ok := ipfsCaches[pathkey]; ok {
		preloadNextSegment(clientID, ipfscache, fullpath)
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
			_, quality := ipfscache.ParseSegmentQuality(fullpath)
			clientVideoBandwidth := ipfscache.QualitysBandwidth(quality)
			cachedSet, latest := ipfscache.Latest(clientID)
			log.Println("For segment", latest, ":", cachedSet)

			var off uint64 = 0
			for _, p := range mpdv.Period {
				for _, adapt := range p.AdaptationSets {
					var queue []mpd.Representation
					for i, _ := range adapt.Representations {
						representation := &adapt.Representations[i]
						representation.SegmentTemplate.PresentationTimeOffset = &off

						if cachedSet.Has(Stoi(*representation.ID)) &&
							float64(*representation.Bandwidth) < clientBandwidth[clientID] {
							queue = append(queue, *representation)
						}
					}

					for index, r := range queue {
						newid := *r.ID + ".0000001"
						newbw := uint64(uncachedThroughput + float64(index))
						log.Println("Add", newid, *r.Bandwidth, "=>", uncachedThroughput, newbw)

						adapt.Representations = append(adapt.Representations, mpd.Representation{
							ID:                 &newid,
							MimeType:           r.MimeType,
							Width:              r.Width,
							Height:             r.Height,
							FrameRate:          r.FrameRate,
							Bandwidth:          &newbw,
							AudioSamplingRate:  r.AudioSamplingRate,
							Codecs:             r.Codecs,
							SAR:                r.SAR,
							ScanType:           r.ScanType,
							ContentProtections: r.ContentProtections,
							SegmentTemplate:    r.SegmentTemplate,
							BaseURL:            r.BaseURL,
						})

						newidcur := *r.ID + ".0000002"
						newbwcur := uint64(clientVideoBandwidth + float64(index))
						log.Println("Add", newidcur, *r.Bandwidth, "=>", newbwcur, clientVideoBandwidth)

						adapt.Representations = append(adapt.Representations, mpd.Representation{
							ID:                 &newidcur,
							MimeType:           r.MimeType,
							Width:              r.Width,
							Height:             r.Height,
							FrameRate:          r.FrameRate,
							Bandwidth:          &newbwcur,
							AudioSamplingRate:  r.AudioSamplingRate,
							Codecs:             r.Codecs,
							SAR:                r.SAR,
							ScanType:           r.ScanType,
							ContentProtections: r.ContentProtections,
							SegmentTemplate:    r.SegmentTemplate,
							BaseURL:            r.BaseURL,
						})
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

	t := time.Now()
	proxy.ServeHTTP(c.Writer, c.Request)
	c.Writer.Flush()
	clientLatestTransmit[clientID] = time.Since(t)
	if ipfscache, ok := ipfsCaches[pathkey]; ok {
		var curBW float64 = 10 * 1000 * 1000
		if clientLatestTransmit[clientID].Milliseconds() > 800 {
			currentBandwidthNS := float64(c.Writer.Size()*8) / float64(clientLatestTransmit[clientID].Nanoseconds())
			curBW = currentBandwidthNS * float64(time.Second) * float64(time.Nanosecond)
		} else if clientLatestTransmit[clientID].Milliseconds() < 50 {
			curBW = 16 * 1000 * 1000
		}

		if ipfscache.AlreadyCachedUrl(fullpath) {
			clientBandwidth[clientID] = deltaRate*clientBandwidth[clientID] + (1.0-deltaRate)*curBW
			log.Println("Update clientBandwidth", int64(clientBandwidth[clientID]/1000), "kbits")
		} else {
			updateUncachedThroughput(curBW)
		}
		ipfscache.AddRecordFromURL(fullpath, clientID)
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
	clientLatestTransmit = make(map[string]time.Duration)
	clientBandwidth = make(map[string]float64)
	//	busyChan = make(chan emptyT, 100000)
	// busyQueue = lane.NewDeque()

	go requestBackend()

	r := gin.Default()
	//	r.Use(func(c *gin.Context) {
	//		FrontendBandwidthEstimate(c)
	//	})

	r.Any("/*path", proxyHandle)
	//	r.Any("/*path", pureProxyHandle)

	s := http.Server{
		Addr:    os.Args[2],
		Handler: r,
	}
	s.SetKeepAlivesEnabled(false)
	s.ListenAndServe()
}
