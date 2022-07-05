package redis

import (
	"container/list"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	redigo "github.com/garyburd/redigo/redis"
)


type Client struct {
	conn redigo.Conn
	Addr string
	Auth string

	Database int

	LastUse time.Time
	Timeout time.Duration
}

type SlowlogInfo struct {
	ID 			int64
	TimeStamp 	int64
	CostTime 	int64
	CmdInfo 	[]string
}

type KeyInfo struct {
	Key           string `json:"key"`
	Batchsize     int64  `json:"batchsize,omitempty"`   //针对数据类型为：[list，set，zset，hash]，被涉及到的成员数量
	MaxMemberSize int64  `json:"max_mem_size,omitempty"`  //针对数据类型为hash的情况，对应涉及到成员中，最大的成员大小
	DataSize      int64  `json:"datasize,omitempty"`    //针对数据类型为string的字节长度，以及数据类型为hash的操作的成员总字节长度（该长度可能不是key的总大小）
}

type KeyDetail struct {
	NumOfOperatedKeys    int64         `json:"key_num,omitempty"`
	KeyInfoList          []KeyInfo     `json:"keys_info,omitempty"`
	ResponseBatchsize    int64         `json:"response_batchsize,omitempty"`  //特殊情况，针对redis响应集合的大小
}

type MonitorRecord struct {
	ID                        int64       `json:"id"`
	TimeStamp                 string      `json:"ts"`
	CmdName                   string      `json:"cmd_name"`
	Cmdinfo                   string      `json:"cmd_info"`
	RemoteAddr                string      `json:"remote_addr"`
	AbnormalType              int         `json:"type"`
	Details                   KeyDetail   `json:"details,omitempty"`
	DelayUs                   int64       `json:"delay_us,omitempty"`
}


var DefaultPwd = "jredis123456"

var CmdList = [...]string{"info","client","config"}
//运维操作指令
var OpsCmd =[...]string{"cmdstat_auth", "cmdstat_client","cmdstat_config",
	"cmdstat_info","cmdstat_latency","cmdstat_slowlog",
	"cmdstat_select","cmdstat_ping","cmdstat_bgsave",
	"cmdstat_psync","cmdstat_replconf","cmdstat_monitor",
	"cmdstat_slaveof","cmdstat_time","cmdstat_command"}

var PikaOpsCmd=[...]string{"ALL","AUTH","PING","SLOWLOG","CLIENT","CONFIG","INFO","PUBLISH","TRYSYNC"}

func NewClientNoAuth(addr string, timeout time.Duration) (*Client, error) {
	return NewClient(addr, "", timeout)
}

func NewClient(addr string, auth string, timeout time.Duration) (*Client, error) {
	c, err := redigo.Dial("tcp", addr, []redigo.DialOption{
		redigo.DialConnectTimeout(timeout),
		redigo.DialPassword(auth),
		redigo.DialReadTimeout(timeout), redigo.DialWriteTimeout(timeout),
	}...)
	if err != nil {
		return nil, err
	}
	return &Client{
		conn: c, Addr: addr, Auth: auth,
		LastUse: time.Now(), Timeout: timeout,
	}, nil
}

func (c *Client) Close() error {
	return c.conn.Close()
}

func (c *Client) Do(cmd string, args ...interface{}) (interface{}, error) {
	r, err := c.conn.Do(cmd, args...)
	if err != nil {
		c.Close()
		return nil, err
	}
	c.LastUse = time.Now()

	if err, ok := r.(redigo.Error); ok {
		return nil, err
	}
	return r, nil
}

func (c *Client) Send(cmd string, args ...interface{}) error {
	if err := c.conn.Send(cmd, args...); err != nil {
		c.Close()
		return err
	}
	return nil
}

func (c *Client) Flush() error {
	if err := c.conn.Flush(); err != nil {
		c.Close()
		return err
	}
	return nil
}

func (c *Client) Receive() (interface{}, error) {
	r, err := c.conn.Receive()
	if err != nil {
		c.Close()
		return nil, err
	}
	c.LastUse = time.Now()

	if err, ok := r.(redigo.Error); ok {
		return nil, err
	}
	return r, nil
}

func (c *Client) Select(database int) error {
	if c.Database == database {
		return nil
	}
	_, err := c.Do("SELECT", database)
	if err != nil {
		c.Close()
		return err
	}
	c.Database = database
	return nil
}

func (c *Client) Shutdown() error {
	_, err := c.Do("SHUTDOWN")
	if err != nil {
		c.Close()
		return err
	}
	return nil
}

func (c *Client) Info() (map[string]string, error) {
	text, err := redigo.String(c.Do("INFO"))
	if err != nil {
		return nil, err
	}
	info := make(map[string]string)
	for _, line := range strings.Split(text, "\n") {
		kv := strings.SplitN(line, ":", 2)
		if len(kv) != 2 {
			continue
		}
		if key := strings.TrimSpace(kv[0]); key != "" {
			info[key] = strings.TrimSpace(kv[1])
		}
	}
	return info, nil
}

//参数用空格分隔
func (c *Client) ConfigGet(args string) (map[string]string,error) {
	cmd := "config "+ args
	result,err := c.DoCmd_Map(cmd)
	if err != nil {
		return nil,err
	}
	return result,nil
}

func (c *Client)XconfigGet(args string)(map[string]string,error)  {
	cmd := "xconfig " + args
	result,err := c.DoCmd_Map(cmd)
	if err != nil {
		return nil,err
	}
	return result,nil
}

func (c *Client) DoCmdLine(line string) error {
	cmd, args, err := Parse(line)
	if err != nil{
		return err
	}
	_,err = c.Do(cmd,args...)
	return err
}

func (c *Client) DoCmd_string(cmd string) (string, error) {
	cmd_arr := strings.Split(cmd, " ")
	params_arr := cmd_arr[1:]
	params := make([]interface{}, len(params_arr))
	for i, v := range params_arr {
		params[i] = v
	}
	text, err := redigo.String(c.Do(cmd_arr[0], params...))
	if err != nil {
		return "", err
	}
	return text, nil
}

func (c *Client) DoCmd_int(cmd string) (int, error) {
	cmd_arr := strings.Split(cmd, " ")
	params_arr := cmd_arr[1:]
	params := make([]interface{}, len(params_arr))
	for i, v := range params_arr {
		params[i] = v
	}
	ret, err := redigo.Int(c.Do(cmd_arr[0], params...))
	if err != nil {
		return -1, err
	}
	return ret, nil
}

func (c *Client) DoCmd_strings(cmd string) ([]string, error) {
	cmd_arr := strings.Split(cmd, " ")
	params_arr := cmd_arr[1:]
	params := make([]interface{}, len(params_arr))
	for i, v := range params_arr {
		params[i] = v
	}
	text, err := redigo.Strings(c.Do(cmd_arr[0], params...))
	if err != nil {
		return nil, err
	}
	return text, nil
}

func (c *Client)DoCmd_Map(cmd string)(map[string]string, error)  {
	cmd_arr := strings.Split(cmd, " ")
	params_arr := cmd_arr[1:]
	params := make([]interface{}, len(params_arr))
	for i, v := range params_arr {
		params[i] = v
	}
	result, err := c.Do(cmd_arr[0], params...)
	if err != nil{
		return nil,err
	}
	ret := make(map[string]string,0)
	if arr,ok := result.([]interface{});ok{
		for idx, v := range arr{
			if idx%2 == 1{
				rawValue := v
				rawKey := arr[idx-1]
				rawKeyBts := rawKey.([]byte)
				rawValueBts := rawValue.([]byte)
				ret[string(rawKeyBts)] = string(rawValueBts)
			}
		}
	}

	return ret,nil
}

var ErrClosedPool = errors.New("use of closed redis pool")

type Pool struct {
	mu sync.Mutex

	auth string
	pool map[string]*list.List

	timeout time.Duration

	exit struct {
		C chan struct{}
	}

	closed bool
}

func NewPool(auth string, timeout time.Duration) *Pool {
	p := &Pool{
		auth: auth, timeout: timeout,
		pool: make(map[string]*list.List),
	}
	p.exit.C = make(chan struct{})

	if timeout != 0 {
		go func() {
			var ticker = time.NewTicker(time.Minute)
			defer ticker.Stop()
			for {
				select {
				case <-p.exit.C:
					return
				case <-ticker.C:
					p.Cleanup()
				}
			}
		}()
	}

	return p
}

func (p *Pool) isRecyclable(c *Client) bool {
	if c.conn.Err() != nil {
		return false
	}
	return p.timeout == 0 || time.Since(c.LastUse) < p.timeout
}

func (p *Pool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil
	}
	p.closed = true
	close(p.exit.C)

	for addr, list := range p.pool {
		for i := list.Len(); i != 0; i-- {
			c := list.Remove(list.Front()).(*Client)
			c.Close()
		}
		delete(p.pool, addr)
	}
	return nil
}

func (p *Pool) Cleanup() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return ErrClosedPool
	}

	for addr, list := range p.pool {
		for i := list.Len(); i != 0; i-- {
			c := list.Remove(list.Front()).(*Client)
			if p.isRecyclable(c) {
				list.PushBack(c)
			} else {
				c.Close()
			}
		}
		if list.Len() == 0 {
			delete(p.pool, addr)
		}
	}
	return nil
}

func (p *Pool) GetClient(addr string) (*Client, error) {
	c, err := p.getClientFromCache(addr)
	if err != nil || c != nil {
		return c, err
	}
	return NewClient(addr, p.auth, p.timeout)
}

func (p *Pool) getClientFromCache(addr string) (*Client, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, ErrClosedPool
	}
	if list := p.pool[addr]; list != nil {
		for i := list.Len(); i != 0; i-- {
			c := list.Remove(list.Front()).(*Client)
			if p.isRecyclable(c) {
				return c, nil
			} else {
				c.Close()
			}
		}
	}
	return nil, nil
}

func (p *Pool) PutClient(c *Client, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err != nil || p.closed || !p.isRecyclable(c) {
		c.Close()
	} else {
		cache := p.pool[c.Addr]
		if cache == nil {
			cache = list.New()
			p.pool[c.Addr] = cache
		}
		cache.PushFront(c)
	}
}

func (p *Pool) Info(addr string) (_ map[string]string, err error) {
	c, err := p.GetClient(addr)
	if err != nil {
		return nil, err
	}
	defer p.PutClient(c, err)
	m, err := c.Info()
	if err != nil {
		return nil, err
	}
	return m, nil
}

type InfoCache struct {
	mu sync.Mutex

	Auth string
	data map[string]map[string]string

	Timeout time.Duration
}

func (s *InfoCache) load(addr string) map[string]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.data != nil {
		return s.data[addr]
	}
	return nil
}

func (s *InfoCache) store(addr string, info map[string]string) map[string]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.data == nil {
		s.data = make(map[string]map[string]string)
	}
	if info != nil {
		s.data[addr] = info
	} else if s.data[addr] == nil {
		s.data[addr] = make(map[string]string)
	}
	return s.data[addr]
}

func (s *InfoCache) Get(addr string) map[string]string {
	info := s.load(addr)
	if info != nil {
		return info
	}
	info, _ = s.getSlow(addr)
	return s.store(addr, info)
}

func (s *InfoCache) GetRunId(addr string) string {
	return s.Get(addr)["run_id"]
}

func (s *InfoCache) getSlow(addr string) (map[string]string, error) {
	c, err := NewClient(addr, s.Auth, s.Timeout)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	return c.Info()
}

func (c *Client) ConfigSet(configKey, configValue string) error {

	if _, err := c.Do("CONFIG", "SET", configKey, configValue); err != nil {
		return err
	}

	if _, err := c.Do("CONFIG", "REWRITE"); err != nil {
		return err
	}

	return nil
}

func (c *Client) Set(k, v string) error {
	if _, err := c.Do("SET", k, v); err != nil {
		return err
	}
	return nil
}

func (c *Client) Setex(k, v string, ttl int) error {
	if _, err := c.Do("SET", k, v, "EX", ttl); err != nil {
		return err
	}
	return nil
}

func (c *Client) Zadd(k string, score int64, member string) error {
	if _, err := c.Do("ZADD", k, score, member); err != nil {
		return err
	}
	return nil
}

func (c *Client) Del(k string) error {
	if _, err := c.Do("DEL", k); err != nil {
		return err
	}
	return nil
}

func (c *Client) XConfigSet(configKey, configValue string) error {

	if _, err := c.Do("XCONFIG","SET", configKey, configValue); err != nil {
		return err
	}

	if _, err := c.Do("XCONFIG","REWRITE"); err != nil {
		return err
	}

	return nil
}

func Parse(cmdStr string)(cmd string,args []interface{},err error)  {
	var (
		cmdArr []string
		cCmd  = ""
	)
	if len(cmdStr) == 0 {
		return "",nil,fmt.Errorf("cmd string is empty")
	}
	args = make([]interface{},0)
	//遍历
	for idx, v := range cmdStr{
		if v != ' '{
			cCmd += string(v)
		}else{
			if len(cCmd) > 0 {
				cmdArr = append(cmdArr,cCmd)
				cCmd = ""
			}
		}
		if idx == len(cmdStr)-1{
			if len(cmdStr) > 0 {
				cmdArr = append(cmdArr,cCmd)
			}
		}
	}
	if len(cmdArr) == 0{
		return "",nil,fmt.Errorf("parse empty cmd string")
	}
	cmd = cmdArr[0]
	for i := 1; i < len(cmdArr);i++{
		args = append(args,cmdArr[i])
	}
	return
}



