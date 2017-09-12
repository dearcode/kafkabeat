package config

import (
	"encoding/json"
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/zssky/log"

	"github.com/dearcode/crab/http/client"
)

var (
	domain = flag.String("domain", "", "tracker manager domain.")
)

const (
	httpTimeout = time.Second * 3
)

//LoadTopics load topics from manager server.
func LoadTopics() (string, error) {
	if *domain == "" {
		return "", nil
	}

	url := fmt.Sprintf("http://%v/api/modules/", *domain)

	buf, _, err := client.NewClient(httpTimeout).Get(url, nil, nil)

	if err != nil {
		log.Errorf("Get modules error:%v, domain:%v", err, *domain)
		return "", err
	}

	var ns []string
	if err = json.Unmarshal(buf, &ns); err != nil {
		log.Errorf("Unmarshal error:%v, buf:%s", err, buf)
		return "", err
	}

	return strings.Join(ns, ","), nil
}
