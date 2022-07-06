package helper

import (
	"errors"
	"fmt"
	"github.com/emirpasic/gods/sets/treeset"
	"github.com/hdt3213/rdb/bytefmt"
	"github.com/hdt3213/rdb/core"
	"github.com/hdt3213/rdb/model"
	"github.com/hdt3213/rdb/utils/base_func"
	"github.com/hdt3213/rdb/utils/redis"
	"os"
	"strings"
	"time"
)

const (
	ExpRange_None 		= 0 // 不过期
	ExpRange_Stale 		= 1	// 已过期
	ExpRange_1h 		= 2 // 0-1h
	ExpRange_3h 		= 3 // 1h-3h
	ExpRange_12h 		= 4	// 3h-12h
	ExpRange_24h 		= 5	// 12h-24h
	ExpRange_2d 		= 6	// 1-2d
	ExpRange_7d 		= 7	// 3-7d
	ExpRange_Beyond_7d 	= 8	// >7d
)

type RdbStatus struct {
	Code 	int `json:"code"`
	Msg 	string `json:"msg"`
}

type PrefixCounter struct {
	Prefix 			string `json:"prefix"`
	Size 			int64 `json:"size"`
	ReadableSize 	string `json:"readable_size"`
	Count 			int64 `json:"count"`
}

type keyPrefixTreeSet struct {
	set      *treeset.Set
	capacity int
}

type ExpiredKeyCount struct {
	Category string
}

func newKeyPrefixHeap(cap int) *keyPrefixTreeSet {
	s := treeset.NewWith(func(a, b interface{}) int {
		o1 := a.(*PrefixCounter)
		o2 := b.(*PrefixCounter)
		switch {
		case o2.Size > o1.Size:
			return 1
		case o2.Size < o1.Size:
			return -1
		default:
			return 0
		}
	})
	return &keyPrefixTreeSet{
		set:      s,
		capacity: cap,
	}
}

func (p *keyPrefixTreeSet) GetMin() *PrefixCounter {
	iter := p.set.Iterator()
	iter.End()
	if iter.Prev() {
		raw := iter.Value()
		return raw.(*PrefixCounter)
	}
	return nil
}

func (p *keyPrefixTreeSet) Append(x *PrefixCounter) {
	// if heap is full && x.Size > minSize, then pop min
	if p.set.Size() >= p.capacity {
		min := p.GetMin()
		if min.Size < x.Size {
			p.set.Remove(min)
			p.set.Add(x)
		}
	} else {
		p.set.Add(x)
	}
}

type BigKey struct {
	DbIndex 		int `json:"db_index"`
	Key 			string `json:"key"`
	Type 			string `json:"type"`
	Size 			int `json:"size"`
	ReadableSize 	string `json:"readable_size"`
	ElementCount 	int `json:"element_count"`
}

type BiggestKeys struct {
	Keys []*BigKey `json:"keys"`
}

func storeStatusToRedis(c *redis.Client, status *RdbStatus) error {
	v, err := base_func.Any2String(status)
	if err != nil {
		return err
	}
	k := "rdb_status_" + c.Addr
	return c.Setex(k, v, 2592000)
}

func newTopKeyHeap(cap int) *redisTreeSet {
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

func getKeyPrefix(subKey []string, sep string) string {
	// 没有前缀的key，归类为others
	if len(subKey) == 1 {
		return "others"
	}

	// 默认删除最后一个元素
	prefix := ""
	for i := 0; i < len(subKey) - 1; i++ {
		prefix = prefix + subKey[i] + sep
	}
	prefix = strings.TrimRight(prefix, sep)
	return prefix
}

func getExpRange(object model.RedisObject) int {
	expAt := object.GetExpiration()
	if expAt == nil {
		return ExpRange_None
	}
	now := time.Now().Unix()

	ttl := expAt.Unix() - now
	if ttl < 0 {
		return ExpRange_Stale
	} else if ttl < 3600 { // 1h
		return ExpRange_1h
	} else if ttl < 3 * 3600 {
		return ExpRange_3h
	} else if ttl < 12 * 3600 {
		return ExpRange_12h
	} else if ttl < 24 * 3600 {
		return ExpRange_24h
	} else if ttl < 2 * 24 * 3600 {
		return ExpRange_2d
	} else if ttl < 7 * 24 * 3600 {
		return ExpRange_7d
	} else {
		return ExpRange_Beyond_7d
	}
}

func createExpKeyCount(data map[int]int64, object model.RedisObject) {
	data[getExpRange(object)] += 1
}

func storeExpKeyCount(c *redis.Client, data map[int]int64) error {
	v, err := base_func.Any2String(data)
	if err != nil {
		return err
	}

	k := "rdb_exp_key_count_" + c.Addr
	return c.Setex(k, v, 2592000) // ttl 30天
}

func createExpKeyMem(data map[int]int64, object model.RedisObject) {
	data[getExpRange(object)] += int64(object.GetSize())
}

func storeExpKeyMem(c *redis.Client, data map[int]int64) error {
	v, err := base_func.Any2String(data)
	if err != nil {
		return err
	}

	k := "rdb_exp_key_mem_" + c.Addr
	return c.Setex(k, v, 2592000) // ttl 30天
}

func createKeyTypeCount(data map[string]int64, object model.RedisObject) {
	keyType := object.GetType()
	data[keyType] += 1
}

func storeKeyTypeCount(c *redis.Client, data map[string]int64) error {
	v, err := base_func.Any2String(data)
	if err != nil {
		return err
	}

	k := "rdb_key_count_" + c.Addr
	return c.Setex(k, v, 2592000) // ttl 30天
}

func createKeyTypeMem(data map[string]int64, object model.RedisObject) {
	keyType := object.GetType()
	data[keyType] += int64(object.GetSize())
}

func storeKeyTypeMem(c *redis.Client, data map[string]int64) error {
	v, err := base_func.Any2String(data)
	if err != nil {
		return err
	}

	k := "rdb_key_Mem_" + c.Addr
	return c.Setex(k, v, 2592000) // ttl 30天
}

func storeTopKey(c *redis.Client, data *redisTreeSet) error {
	ret := &BiggestKeys{}
	ret.Keys = make([]*BigKey, 0)
	iter := data.set.Iterator()
	for iter.Next() {
		object := iter.Value().(model.RedisObject)
		//exp := object.GetExpiration()
		//if exp != nil {
		//	fmt.Printf("key:%s, ttl:%s\n", object.GetKey(), object.GetExpiration().Unix())
		//}
		k := &BigKey{
			DbIndex: object.GetDBIndex(),
			Key: object.GetKey(),
			Type: object.GetType(),
			Size: object.GetSize(),
			ReadableSize: bytefmt.FormatSize(uint64(object.GetSize())),
			ElementCount: object.GetElemCount(),
		}
		ret.Keys = append(ret.Keys, k)
	}

	v, err := base_func.Any2String(ret)
	if err != nil {
		return err
	}
	k := "rdb_bk_data_" + c.Addr
	return c.Setex(k, v, 2592000) // ttl 30天
}

func createKeyPrefix(data map[string]*PrefixCounter, sep string, object model.RedisObject) {
	subKey := strings.Split(object.GetKey(), sep)
	prefix := getKeyPrefix(subKey, sep)
	if _, ok := data[prefix]; !ok {
		data[prefix] = &PrefixCounter{}
		data[prefix].Prefix = prefix
		data[prefix].Size = int64(object.GetSize())
		data[prefix].Count = 1
	} else {
		data[prefix].Size += int64(object.GetSize())
		data[prefix].Count++
	}
}

func storeKeyPrefix(c *redis.Client, data map[string]*PrefixCounter, topN int) error {
	if len(data) == 0 {
		return nil
	}

	// 删除旧数据
	k := "rdb_prefix_data_" + c.Addr
	err := c.Del(k)
	if err != nil {
		return err
	}

	topList := newKeyPrefixHeap(topN)
	for _, v := range data {
		topList.Append(v)
	}

	iter := topList.set.Iterator()
	for iter.Next() {
		item := iter.Value().(*PrefixCounter)
		item.ReadableSize = bytefmt.FormatSize(uint64(item.Size))

		v, err := base_func.Any2String(item)
		if err != nil {
			return err
		}
		err = c.Zadd(k, item.Size, v)
		if err != nil {
			return err
		}
	}
	return nil
}

func CreateAll(rdbFilename string, topN int, separators []string, addr, auth string, options ...interface{}) error {
	if addr == "" {
		return errors.New("redis addr can not empty")
	}
	c, err := redis.NewClient(addr, auth, time.Millisecond * 100)
	if err != nil {
		return errors.New(err.Error())
	}

	status := &RdbStatus{}
	if rdbFilename == "" {
		status.Code = -1
		status.Msg = "src file path is required"
		return storeStatusToRedis(c, status)
	}
	if len(separators) != 1 {
		status.Code = -1
		status.Msg = "only support one separators"
		return storeStatusToRedis(c, status)
	}

	if topN <= 0 || topN > 10000{
		topN = 100
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

	expKeyCount := make(map[int]int64)					// key过期时间分布（数量）
	expKeyMem := make(map[int]int64)					// key过期时间分布（内存）
	keyCount := make(map[string]int64) 					// 不同类型key数量
	keyMemory := make(map[string]int64) 				// 不同类型key内存总量
	topList := newTopKeyHeap(topN) 						// 大key
	keyPrefixMap := make(map[string]*PrefixCounter) 	// 前缀统计

	err = dec.Parse(func(object model.RedisObject) bool {
		createExpKeyCount(expKeyCount, object)
		createExpKeyMem(expKeyMem, object)
		createKeyTypeCount(keyCount, object)
		createKeyTypeMem(keyMemory, object)
		topList.Append(object)
		createKeyPrefix(keyPrefixMap, separators[0], object)
		return true
	})
	if err != nil {
		status.Code = -1
		status.Msg = err.Error()
		return storeStatusToRedis(c, status)
	}

	// refresh client
	c, err = redis.NewClient(addr, auth, time.Millisecond * 100)
	if err != nil {
		return errors.New(err.Error())
	}

	// 写redis
	err = storeExpKeyCount(c, expKeyCount)
	if err != nil {
		status.Code = -1
		status.Msg = err.Error()
		return storeStatusToRedis(c, status)
	}

	err = storeExpKeyMem(c, expKeyMem)
	if err != nil {
		status.Code = -1
		status.Msg = err.Error()
		return storeStatusToRedis(c, status)
	}

	err = storeKeyTypeCount(c, keyCount)
	if err != nil {
		status.Code = -1
		status.Msg = err.Error()
		return storeStatusToRedis(c, status)
	}

	err = storeKeyTypeMem(c, keyMemory)
	if err != nil {
		status.Code = -1
		status.Msg = err.Error()
		return storeStatusToRedis(c, status)
	}

	err = storeTopKey(c, topList)
	if err != nil {
		status.Code = -1
		status.Msg = err.Error()
		return storeStatusToRedis(c, status)
	}

	err = storeKeyPrefix(c, keyPrefixMap, topN)
	if err != nil {
		status.Code = -1
		status.Msg = err.Error()
		return storeStatusToRedis(c, status)
	}

	status.Code = 0
	status.Msg = "ok"
	return storeStatusToRedis(c, status)
}
