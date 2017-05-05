package beater

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/elastic/beats/libbeat/beat"
	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/libbeat/publisher"

	"github.com/dearcode/kafkabeat/config"
)

type Kafkabeat struct {
	ctx      context.Context
	cancel   context.CancelFunc
	config   config.Config
	pc       publisher.Client
	consumer *cluster.Consumer
}

// Creates beater
func New(b *beat.Beat, cfg *common.Config) (beat.Beater, error) {
	config := config.DefaultConfig
	if err := cfg.Unpack(&config); err != nil {
		return nil, fmt.Errorf("Error reading config file: %v", err)
	}

	cc := cluster.NewConfig()
	cc.Consumer.Return.Errors = true
	cc.Group.Return.Notifications = true

	consumer, err := cluster.NewConsumer(config.Brokers, config.Group, config.Topics, cc)
	if err != nil {
		return nil, fmt.Errorf("Error NewConsumer: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	kb := &Kafkabeat{
		ctx:      ctx,
		cancel:   cancel,
		config:   config,
		pc:       b.Publisher.Connect(),
		consumer: consumer,
	}
	return kb, nil
}

func (kb *Kafkabeat) decodeMessage(msg []byte) (common.MapStr, error) {
	var m common.MapStr

	if err := json.Unmarshal(msg, &m); err != nil {
		return nil, err
	}

	if to, err := m.GetValue("@timestamp"); err == nil {
		if tss, ok := to.(string); ok {
			t, err := common.ParseTime(tss)
			if err != nil {
				return nil, err
			}
			m.Put("@timestamp", t)
		}
	}

	return m, nil
}

func (kb *Kafkabeat) sendMessage(msg *sarama.ConsumerMessage) error {
	var m common.MapStr

	if err := json.Unmarshal(msg.Value, &m); err != nil {
		return err
	}

	if to, err := m.GetValue("@timestamp"); err == nil {
		if tss, ok := to.(string); ok {
			t, err := common.ParseTime(tss)
			if err != nil {
				return err
			}
			m.Put("@timestamp", t)
		}
	}

	kb.pc.PublishEvent(m, publisher.Sync)
	kb.consumer.MarkOffset(msg, "")

	return nil
}

func (kb *Kafkabeat) Run(b *beat.Beat) error {
	logp.Info("kafkabeat is running! Hit CTRL-C to stop it.")

	for {
		select {
		case <-kb.ctx.Done():
			logp.Info("kafkabeat stop.")
			kb.consumer.Close()

			if msg, ok := <-kb.consumer.Messages(); ok {
				if err := kb.sendMessage(msg); err != nil {
					logp.Err("kafkabeat push message error:%v, value:%s", err, msg.Value)
					continue
				}
			}

			return nil

		case msg, ok := <-kb.consumer.Messages():
			if ok {
				if err := kb.sendMessage(msg); err != nil {
					logp.Err("kafkabeat push message error:%v, value:%s", err, msg.Value)
					continue
				}
			}
		case err, ok := <-kb.consumer.Errors():
			if ok {
				logp.Info("Error: %s\n", err.Error())
			}
		case ntf, ok := <-kb.consumer.Notifications():
			if ok {
				logp.Info("Rebalanced: %+v\n", ntf)
			}
		}
	}
}

func (kb *Kafkabeat) Stop() {
	kb.pc.Close()
	kb.cancel()
}
