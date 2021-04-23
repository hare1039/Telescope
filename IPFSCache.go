package main

import (
	"errors"
	"fmt"
	"log"
	"path"
	"strconv"
	"strings"

	"github.com/scylladb/go-set/strset"
	"github.com/unki2aut/go-mpd"
)

type Matcher struct {
	ID        string
	Suffix    string
	Bandwidth float64
}

type IPFSCache struct {
	// every mpd(url.URL) at number
	IPFSCachedSegments map[uint64]*strset.Set
	mpdTree            mpd.MPD
	URLMatcher         map[string]Matcher
	LatestProgress     map[string]uint64
	MaxSegmentNumber   uint64
	ShouldEnd          map[string]bool
}

func NewIPFSCache(m *mpd.MPD) *IPFSCache {
	c := new(IPFSCache)
	c.IPFSCachedSegments = make(map[uint64]*strset.Set)
	c.URLMatcher = make(map[string]Matcher)
	c.mpdTree = *m
	c.LatestProgress = make(map[string]uint64)
	c.PrepareURLMatcher()
	c.ShouldEnd = make(map[string]bool)
	return c
}

func (c *IPFSCache) PrepareURLMatcher() {
	c.MaxSegmentNumber = 0
	var segmentLength uint64
	for _, p := range c.mpdTree.Period {
		for _, adapt := range p.AdaptationSets {
			for i, _ := range adapt.Representations {
				representation := &adapt.Representations[i]
				pos := strings.LastIndex(*representation.SegmentTemplate.Media, "$Number$")
				segmentLength = *representation.SegmentTemplate.Duration / *representation.SegmentTemplate.Timescale

				c.URLMatcher[(*representation.SegmentTemplate.Media)[:pos]] = Matcher{
					ID:        *representation.ID,
					Suffix:    (*representation.SegmentTemplate.Media)[pos+len("$Number$"):],
					Bandwidth: float64(*representation.Bandwidth),
				}
			}
		}

		leng, _ := p.Duration.ToSeconds()
		var lengInt uint64 = uint64(leng)
		c.MaxSegmentNumber += lengInt / segmentLength
	}
}

func (c *IPFSCache) AlreadyCached(url string) bool {
	ID, number := c.ParseIDNumber(url)
	if number == 0 || c.IPFSCachedSegments[number] == nil {
		return false
	}

	return c.IPFSCachedSegments[number].Has(ID)
}

func (c *IPFSCache) ParseIDNumber(url string) (string, uint64) {
	var ID string
	var number uint64
	url = path.Base(url)
	for key, value := range c.URLMatcher {
		if strings.HasPrefix(url, key) {
			url = strings.TrimPrefix(url, key)
			n, _ := strconv.Atoi(strings.TrimSuffix(url, value.Suffix))
			number = uint64(n)
			ID = value.ID
		}
	}
	return ID, number
}

func (c *IPFSCache) AddRecordFromURL(ipaddress string, url string) error {
	ID, number := c.ParseIDNumber(url)
	if number == c.MaxSegmentNumber {
		c.ShouldEnd[ipaddress] = true
		fmt.Println("throw error:", number, c.MaxSegmentNumber)
		return errors.New("Overheated")
	}

	if number != 0 {
		c.LatestProgress[ipaddress] = number
		c.AddRecord(number, ID)
	}
	return nil
}

func (c *IPFSCache) Latest(ipaddress string) (*strset.Set, uint64) {
	latest := c.LatestProgress[ipaddress] + 1
	if val, ok := c.IPFSCachedSegments[latest]; !ok {
		return &strset.Set{}, latest
	} else {
		return val, latest
	}
}

func (c *IPFSCache) AddRecord(number uint64, representationID string) {
	if _, ok := c.IPFSCachedSegments[number]; !ok {
		c.IPFSCachedSegments[number] = strset.New()
	}

	c.IPFSCachedSegments[number].Add(representationID)
	//	log.Println("Add segment", number, ":", representationID)
}

func (c *IPFSCache) Print() {
	log.Println("x")
}
