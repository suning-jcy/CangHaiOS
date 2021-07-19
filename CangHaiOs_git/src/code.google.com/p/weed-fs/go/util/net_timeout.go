package util

import (
	"code.google.com/p/weed-fs/go/stats"
	"errors"
	"net"
	"time"
)

var StopServerError = errors.New("Server is shutting down!")

// Listener wraps a net.Listener, and gives a place to store the timeout
// parameters. On Accept, it will wrap the net.Conn with our own Conn for us.
type Listener struct {
	net.Listener
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	stopped      *bool
	getstats     bool
}

func (l *Listener) Accept() (net.Conn, error) {
	if l.stopped != nil && *l.stopped {
		return nil, StopServerError
	}
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	if l.getstats == true {
		stats.ConnectionOpen()
	}
	tc := &Conn{
		Conn:         c,
		ReadTimeout:  l.ReadTimeout,
		WriteTimeout: l.WriteTimeout,
		getstats:     l.getstats,
	}
	return tc, nil
}

// Conn wraps a net.Conn, and sets a deadline for every read
// and write operation.
type Conn struct {
	net.Conn
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	getstats     bool
}

func (c *Conn) Read(b []byte) (count int, e error) {
	err := c.Conn.SetReadDeadline(time.Now().Add(c.ReadTimeout))
	if err != nil {
		return 0, err
	}
	count, e = c.Conn.Read(b)
	if e == nil && c.getstats {
		stats.BytesIn(int64(count))
	}
	return
}

func (c *Conn) Write(b []byte) (count int, e error) {
	err := c.Conn.SetWriteDeadline(time.Now().Add(c.WriteTimeout))
	if err != nil {
		return 0, err
	}
	count, e = c.Conn.Write(b)
	if e == nil  && c.getstats {
		stats.BytesOut(int64(count))
	}
	return
}

func (c *Conn) Close() error {
	if c.getstats {
		stats.ConnectionClose()
	}
	return c.Conn.Close()
}

func NewListener(addr string, timeout time.Duration) (net.Listener, error) {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}

	tl := &Listener{
		Listener:     l,
		ReadTimeout:  timeout,
		WriteTimeout: timeout,
	}
	return tl, nil
}
func NewListenerWithStopFlag(addr string, timeout time.Duration, stopFlag *bool) (net.Listener, error) {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}

	tl := &Listener{
		Listener:     l,
		ReadTimeout:  timeout,
		WriteTimeout: timeout,
		stopped:      stopFlag,
	}
	return tl, nil
}
//jjj@20190115 ossfuse中不启用channel构造的统计模块(解决bug: SDOSS-170 存储网关接口返回太长，ossp处理不了)
func NewListenerWithStopFlagWithoutStats(addr string, timeout time.Duration, stopFlag *bool) (net.Listener, error) {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}

	tl := &Listener{
		Listener:     l,
		ReadTimeout:  timeout,
		WriteTimeout: timeout,
		stopped:      stopFlag,
		getstats:     false,
	}
	return tl, nil
}