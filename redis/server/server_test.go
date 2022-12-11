package server

import (
	"bufio"
	"github.com/hdt3213/godis/lib/logger"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/tcp"
	"net"
	"testing"
	"time"
)

func TestStartServer1(t *testing.T) {
	var err error
	closeChan := make(chan struct{})
	listener, err := net.Listen("tcp", ":6379")
	if err != nil {
		t.Error(err)
		return
	}
	go tcp.ListenAndServe(listener, MakeHandler(), closeChan)
	<-closeChan
}

func TestStartServer2(t *testing.T) {
	var err error
	closeChan := make(chan struct{})
	listener, err := net.Listen("tcp", ":6380")
	if err != nil {
		t.Error(err)
		return
	}
	go tcp.ListenAndServe(listener, MakeHandler(), closeChan)
	<-closeChan
}

func TestListenAndServe(t *testing.T) {
	var err error
	closeChan := make(chan struct{})
	listener, err := net.Listen("tcp", ":6379")
	if err != nil {
		t.Error(err)
		return
	}
	addr := listener.Addr().String()
	go tcp.ListenAndServe(listener, MakeHandler(), closeChan)

	// Dial创建一个客户端与上面创建的服务端链接
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Error(err)
		return
	}
	// 客户端发送一个消息，以\r\n结尾
	_, err = conn.Write([]byte("PING\r\n"))
	if err != nil {
		t.Error(err)
		return
	}
	bufReader := bufio.NewReader(conn)
	// 客户端读取一行服务端发的消息
	line, _, err := bufReader.ReadLine()
	if err != nil {
		t.Error(err)
		return
	}
	logger.Info(utils.BytesToString(line))
	if string(line) != "+PONG" {
		t.Error("get wrong response")
		return
	}
	closeChan <- struct{}{}
	time.Sleep(time.Second)
}
