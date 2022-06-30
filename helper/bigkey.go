package helper

import (
	"encoding/csv"
	"errors"
	"fmt"
	"github.com/emirpasic/gods/sets/treeset"
	"github.com/hdt3213/rdb/bytefmt"
	"github.com/hdt3213/rdb/core"
	"github.com/hdt3213/rdb/model"
	"github.com/hdt3213/rdb/utils/base_func"
	"github.com/hdt3213/rdb/utils/redis"
	"os"
	"strconv"
	"time"
)

type redisTreeSet struct {
	set      *treeset.Set
	capacity int
}

func (h *redisTreeSet) GetMin() model.RedisObject {
	iter := h.set.Iterator()
	iter.End()
	if iter.Prev() {
		raw := iter.Value()
		return raw.(model.RedisObject)
	}
	return nil
}

// Append new object into tree set
// time complexity: O(n*log(m)), n is number of redis object, m is heap capacity. m if far less than n
func (h *redisTreeSet) Append(x model.RedisObject) {
	// if heap is full && x.Size > minSize, then pop min
	if h.set.Size() >= h.capacity {
		min := h.GetMin()
		if min.GetSize() < x.GetSize() {
			h.set.Remove(min)
			h.set.Add(x)
		}
	} else {
		h.set.Add(x)
	}
}

func (h *redisTreeSet) Dump() []model.RedisObject {
	result := make([]model.RedisObject, 0, h.set.Size())
	iter := h.set.Iterator()
	for iter.Next() {
		result = append(result, iter.Value().(model.RedisObject))
	}
	return result
}

func newRedisHeap(cap int) *redisTreeSet {
	s := treeset.NewWith(func(a, b interface{}) int {
		o1 := a.(model.RedisObject)
		o2 := b.(model.RedisObject)
		return o2.GetSize() - o1.GetSize() // desc order
	})
	return &redisTreeSet{
		set:      s,
		capacity: cap,
	}
}

// FindBiggestKeys read rdb file and find the largest N keys.
// The invoker owns output, FindBiggestKeys won't close it
func FindBiggestKeys(rdbFilename string, topN int, output *os.File, options ...interface{}) error {
	if rdbFilename == "" {
		return errors.New("src file path is required")
	}
	if topN <= 0 {
		return errors.New("n must greater than 0")
	}
	rdbFile, err := os.Open(rdbFilename)
	if err != nil {
		return fmt.Errorf("open rdb %s failed, %v", rdbFilename, err)
	}
	defer func() {
		_ = rdbFile.Close()
	}()
	var regexOpt RegexOption
	for _, opt := range options {
		switch o := opt.(type) {
		case RegexOption:
			regexOpt = o
		}
	}
	var dec decoder = core.NewDecoder(rdbFile)
	if regexOpt != nil {
		dec, err = regexWrapper(dec, *regexOpt)
		if err != nil {
			return err
		}
	}
	topList := newRedisHeap(topN)
	err = dec.Parse(func(object model.RedisObject) bool {
		topList.Append(object)
		return true
	})
	if err != nil {
		return err
	}
	_, err = output.WriteString("database,key,type,size,size_readable,element_count\n")
	if err != nil {
		return fmt.Errorf("write header failed: %v", err)
	}
	csvWriter := csv.NewWriter(output)
	defer csvWriter.Flush()
	iter := topList.set.Iterator()
	for iter.Next() {
		object := iter.Value().(model.RedisObject)
		err = csvWriter.Write([]string{
			strconv.Itoa(object.GetDBIndex()),
			object.GetKey(),
			object.GetType(),
			strconv.Itoa(object.GetSize()),
			bytefmt.FormatSize(uint64(object.GetSize())),
			strconv.Itoa(object.GetElemCount()),
		})
		if err != nil {
			return fmt.Errorf("csv write failed: %v", err)
		}
	}
	return nil
}

type FindBiggestKeysStatus struct {
	Code int `json:"code"`
	Msg string `json:"msg"`
}

type BigKey struct {
	DbIndex int `json:"db_index"`
	Key string `json:"key"`
	Type string `json:"type"`
	Size int `json:"size"`
	ReadableSize string `json:"readable_size"`
	ElementCount int `json:"element_count"`
}

type BiggestKeys struct {
	Keys []*BigKey `json:"keys"`
}

func getStatusKeyName(addr string) string {
	return addr + "_bk_status"
}

func getBigKeyName(addr string) string {
	return addr + "_bk_data"
}

func storeStatusToRedis(c *redis.Client, status *FindBiggestKeysStatus) error {
	v, err := base_func.Any2String(status)
	if err != nil {
		return err
	}
	return c.Set(getStatusKeyName(c.Addr), v)
}

func storeKeysToRedis(c *redis.Client, keys *BiggestKeys) error {
	v, err := base_func.Any2String(keys)
	if err != nil {
		return err
	}
	return c.Set(getBigKeyName(c.Addr), v)
}

func FindBiggestKeysToRedis(rdbFilename string, topN int, output *os.File, addr, auth string, options ...interface{}) error {
	if addr == "" {
		return errors.New("redis addr can not empty")
	}
	c, err := redis.NewClient(addr, auth, time.Millisecond * 100)
	if err != nil {
		return errors.New(err.Error())
	}

	status := &FindBiggestKeysStatus{}
	if rdbFilename == "" {
		status.Code = -1
		status.Msg = "src file path is required"
		return storeStatusToRedis(c, status)
	}
	if topN <= 0 {
		status.Code = -1
		status.Msg = "n must greater than 0"
		return storeStatusToRedis(c, status)
	}

	// 扫描rdb中
	status.Code = 1
	status.Msg = "parser rdb file..."
	_ = storeStatusToRedis(c, status)

	rdbFile, err := os.Open(rdbFilename)
	if err != nil {
		status.Code = -1
		status.Msg = fmt.Sprintf("open rdb %s failed, %v", rdbFilename, err)
		return storeStatusToRedis(c, status)
	}
	defer func() {
		_ = rdbFile.Close()
	}()
	var regexOpt RegexOption
	for _, opt := range options {
		switch o := opt.(type) {
		case RegexOption:
			regexOpt = o
		}
	}
	var dec decoder = core.NewDecoder(rdbFile)
	if regexOpt != nil {
		dec, err = regexWrapper(dec, *regexOpt)
		if err != nil {
			return err
		}
	}
	topList := newRedisHeap(topN)
	err = dec.Parse(func(object model.RedisObject) bool {
		topList.Append(object)
		return true
	})
	if err != nil {
		status.Code = -1
		status.Msg = err.Error()
		return storeStatusToRedis(c, status)
	}

	data := &BiggestKeys{}
	data.Keys = make([]*BigKey, 0)
	iter := topList.set.Iterator()
	for iter.Next() {
		object := iter.Value().(model.RedisObject)
		k := &BigKey{
			DbIndex: object.GetDBIndex(),
			Key: object.GetKey(),
			Type: object.GetType(),
			Size: object.GetSize(),
			ReadableSize: bytefmt.FormatSize(uint64(object.GetSize())),
			ElementCount: object.GetElemCount(),
		}
		data.Keys = append(data.Keys, k)
	}

	err = storeKeysToRedis(c, data)
	if err != nil {
		status.Code = -1
		status.Msg = err.Error()
		return storeStatusToRedis(c, status)
	}

	status.Code = 0
	status.Msg = "ok"
	return storeStatusToRedis(c, status)
}


