package main

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

var pd = flag.String("pd", "http://127.0.0.1:2379", "pd address")
var minBytes = flag.Uint64("min-bytes", 500000, "min bytes")

type Peer struct {
	Id        uint64 `protobuf:"varint,1,opt,name=id,proto3" json:"id,omitempty"`
	StoreId   uint64 `protobuf:"varint,2,opt,name=store_id,json=storeId,proto3" json:"store_id,omitempty"`
	IsLearner bool   `protobuf:"varint,3,opt,name=is_learner,json=isLearner,proto3" json:"is_learner,omitempty"`
}

type RegionInfo struct {
	ID       uint64  `json:"id"`
	StartKey string  `json:"start_key"`
	EndKey   string  `json:"end_key"`
	Peers    []*Peer `json:"peers,omitempty"`
	Leader   *Peer   `json:"leader,omitempty"`
}

type RegionsInfo struct {
	Count   int           `json:"count"`
	Regions []*RegionInfo `json:"regions"`
}

type StoreInfo struct {
	Store struct {
		ID uint64 `json:"id"`
	} `json:"store"`
}

// StoreHotPeersInfos is used to get human-readable description for hot regions.
type StoreHotPeersInfos struct {
	AsPeer   StoreHotPeersStat `json:"as_peer"`
	AsLeader StoreHotPeersStat `json:"as_leader"`
}

// StoreHotPeersStat is used to record the hot region statistics group by store.
type StoreHotPeersStat map[uint64]*HotPeersStat

type HotPeersStat struct {
	TotalBytesRate float64       `json:"total_flow_bytes"`
	Count          int           `json:"regions_count"`
	Stats          []HotPeerStat `json:"statistics"`
}

// HotPeerStat records each hot peer's statistics
type HotPeerStat struct {
	StoreID  uint64 `json:"store_id"`
	RegionID uint64 `json:"region_id"`

	// HotDegree records the hot region update times
	HotDegree int `json:"hot_degree"`
	// AntiCount used to eliminate some noise when remove region in cache
	AntiCount int `json:"anti_count"`

	Kind     int     `json:"kind"`
	ByteRate float64 `json:"flow_bytes"`
	KeyRate  float64 `json:"flow_keys"`

	// LastUpdateTime used to calculate average write
	LastUpdateTime time.Time `json:"last_update_time"`
	// Version used to check the region split times
	Version uint64 `json:"version"`

	needDelete bool
	isLeader   bool
	isNew      bool
}

type StoresInfo struct {
	Stores []*StoreInfo `json:"stores"`
}

func main() {
	flag.Parse()
	res, err := http.Get(*pd + "/pd/api/v1/hotspot/regions/write")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer res.Body.Close()
	var stores StoreHotPeersInfos
	err = json.NewDecoder(res.Body).Decode(&stores)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("Step1: got hot regions")
	res, err = http.Get(*pd + "/pd/api/v1/regions")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer res.Body.Close()
	var regions RegionsInfo
	err = json.NewDecoder(res.Body).Decode(&regions)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("Step2: got regions")
	print(stores.AsPeer, regions.Regions)
}

func print(stores StoreHotPeersStat, regions []*RegionInfo) {
	sort.Slice(regions, func(i, j int) bool { return regions[i].StartKey < regions[j].StartKey })
	maxKeyLen := 10
	for _, r := range regions {
		r.StartKey, r.EndKey = convertKey(r.StartKey), convertKey(r.EndKey)
		if l := fieldLen(r.StartKey); l > maxKeyLen {
			maxKeyLen = l
		}
		if l := fieldLen(r.EndKey); l > maxKeyLen {
			maxKeyLen = l
		}
	}
	var maxRegionIDLen int
	for _, r := range regions {
		if l := fieldLen(r.ID); l > maxRegionIDLen {
			maxRegionIDLen = l
		}
	}
	var storeIDs []uint64
	for id := range stores {
		storeIDs = append(storeIDs, id)
	}

	sort.Slice(storeIDs, func(i, j int) bool { return storeIDs[i] < storeIDs[j] })
	var storeLen []int
	for id := range stores {
		storeLen = append(storeLen, fieldLen(id))
	}

	field(maxRegionIDLen, "", "")
	for i := range storeIDs {
		field(storeLen[i], "S"+strconv.FormatUint(storeIDs[i], 10), "")
	}
	field(15, "bytes", "")
	field(15, "rate", "")
	fmt.Println()
	isHot := func(region *RegionInfo) (bool, float64, float64) {
		res := false
		for _, peer := range region.Peers {
			hotPeers, ok := stores[peer.StoreId]
			if !ok {
				continue
			}
			for _, hotPeer := range hotPeers.Stats {
				if hotPeer.HotDegree >= 3 && hotPeer.RegionID == region.ID && uint64(hotPeer.ByteRate) > *minBytes {
					return true, hotPeer.ByteRate, hotPeer.KeyRate
				}
			}
		}
		return res, 0, 0
	}

	for _, region := range regions {
		hot, w, r := isHot(region)
		if !hot {
			continue
		}
		field(maxRegionIDLen, "R"+strconv.FormatUint(region.ID, 10), "")
	STORE:
		for i, sid := range storeIDs {
			if region.Leader != nil && sid == region.Leader.StoreId {
				field(storeLen[i], "▀", "\u001b[31m")
				continue
			}
			for _, p := range region.Peers {
				if p.StoreId == sid {
					if p.IsLearner {
						field(storeLen[i], "▀", "\u001b[33m")
					} else {
						field(storeLen[i], "▀", "\u001b[34m")
					}
					continue STORE
				}
			}
			field(storeLen[i], "", "")
		}
		field(15, strconv.FormatUint(uint64(w), 10), "")
		field(15, strconv.FormatUint(uint64(r), 10), "")
		fmt.Println()
	}
}

func convertKey(k string) string {
	b, err := hex.DecodeString(k)
	if err != nil {
		return k
	}
	d, ok := decodeBytes(b)
	if !ok {
		return k
	}
	return strings.ToUpper(hex.EncodeToString(d))
}

func fieldLen(f interface{}) int {
	return len(fmt.Sprintf("%v", f)) + 2
}

func field(l int, s string, color string) {
	slen := utf8.RuneCountInString(s)
	if slen > l {
		fmt.Print(s[:l])
		return
	}
	if slen < l {
		fmt.Print(strings.Repeat(" ", l-slen))
	}
	if color != "" {
		fmt.Print(color)
	}
	fmt.Print(s)
	if color != "" {
		fmt.Print("\u001b[0m")
	}
}

func decodeBytes(b []byte) ([]byte, bool) {
	var buf bytes.Buffer
	for len(b) >= 9 {
		if p := 0xff - b[8]; p >= 0 && p <= 8 {
			buf.Write(b[:8-p])
			b = b[9:]
		} else {
			return nil, false
		}
	}
	return buf.Bytes(), len(b) == 0
}
