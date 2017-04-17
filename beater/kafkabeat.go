package beater

import (
	"context"
	"fmt"
	"strings"

	"github.com/elastic/beats/libbeat/beat"
	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/libbeat/publisher"

	cluster "github.com/bsm/sarama-cluster"

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
	consumer, err := cluster.NewConsumer(strings.Split(config.Brokers, ","), config.Group, strings.Split(config.Topics, ","), cc)
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

func (kb *Kafkabeat) Run(b *beat.Beat) error {
	logp.Info("kafkabeat is running! Hit CTRL-C to stop it.")

	for {
		select {
		case <-kb.ctx.Done():
			logp.Info("kafkabeat stop.")
			kb.consumer.Close()

			if msg, ok := <-kb.consumer.Messages(); ok {
				event := common.MapStr{
					"message": msg,
				}
				kb.pc.PublishEvent(event)
			}

			return nil

		case msg, ok := <-kb.consumer.Messages():
			if ok {
				event := common.MapStr{
					"message": msg,
				}
				kb.pc.PublishEvent(event)
				kb.consumer.MarkOffset(msg, "") // mark message as processed
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
