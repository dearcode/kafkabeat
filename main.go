package main

import (
	"flag"
	"net/http"
	_ "net/http/pprof"
	"os"
	"syscall"
	"time"

	"github.com/dearcode/libbeat/beat"
	"github.com/zssky/log"

	"github.com/dearcode/kafkabeat/beater"
	"github.com/dearcode/kafkabeat/config"
)

func fork(topics string) int {
	attr := syscall.ProcAttr{
		Env:   os.Environ(),
		Files: []uintptr{os.Stdin.Fd(), os.Stdout.Fd(), os.Stderr.Fd()},
	}

	pid, err := syscall.ForkExec(os.Args[0], []string{os.Args[0], "-t", topics}, &attr)
	if err != nil {
		panic(err)
	}

	log.Infof("new child pid:%v, topic:%v", pid, topics)

	return pid
}

func watcher(topics string) {
	if topics == "" {
		return
	}

	t := time.NewTicker(time.Minute)
	pid := fork(topics)

	for {
		<-t.C

		nts, err := config.LoadTopics()
		if err != nil {
			log.Infof("load config error:%v", err)
			continue
		}

		if nts == topics {
			log.Infof("no change:%v", nts)
			continue
		}

		topics = nts

		syscall.Kill(pid, syscall.SIGKILL)

		var wstatus syscall.WaitStatus
		if _, err := syscall.Wait4(pid, &wstatus, 0, nil); err != nil {
			log.Errorf("wait pid:%v error:%v", pid, err)
		} else {
			log.Infof("pid:%v exit status:%v", pid, wstatus)
		}

		pid = fork(topics)
	}
}

func main() {
	flag.Parse()

	topics, err := config.LoadTopics()
	if err != nil {
		log.Infof("load config error:%v", err)
		return
	}

	log.Infof("topics:%v", topics)
	watcher(topics)

	go func() {
		log.Infof("%v", http.ListenAndServe(":9900", nil))
	}()

	if err = beat.Run("kafkabeat", "", beater.New); err != nil {
		log.Infof("run beat error:%v", err)
		os.Exit(1)
	}
}
