package offset

import (
	"container/heap"
	"fmt"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/dearcode/libbeat/logp"
)

type offsetQueue struct {
	offset int64
	queue  []int64
}

func (o offsetQueue) Len() int           { return len(o.queue) }
func (o offsetQueue) Less(i, j int) bool { return o.queue[i] < o.queue[j] }
func (o offsetQueue) Swap(i, j int)      { o.queue[i], o.queue[j] = o.queue[j], o.queue[i] }
func (o *offsetQueue) Top() int64        { return o.queue[0] }

func (o *offsetQueue) Push(x interface{}) {
	o.queue = append(o.queue, x.(int64))
}

func (o *offsetQueue) Pop() interface{} {
	old := o.queue
	n := len(old)
	x := old[n-1]
	o.queue = old[0 : n-1]
	return x
}

type offsetManager struct {
	mq       map[string]*offsetQueue
	consumer *cluster.Consumer
	sync.Mutex
}

func offsetClean(q *offsetQueue) {
	for q.Len() > 1 {
		top := heap.Pop(q).(int64)
		q.offset = top
		if top+1 != q.Top() {
			heap.Push(q, top)
			return
		}
	}
	q.offset = q.Top()

}

var (
	om *offsetManager
)

func Init(consumer *cluster.Consumer) {
	om = &offsetManager{
		mq:       make(map[string]*offsetQueue),
		consumer: consumer,
	}

	go func() {
		t := time.NewTicker(time.Minute)
		for {
			<-t.C
			status()
		}
	}()
}

//InitQueue 如果这个topic,partition不存在就创建并初始化他
func InitQueue(topic string, partition int32, offset int64) {
	om.Lock()
	defer om.Unlock()

	key := fmt.Sprintf("%v_%v", topic, partition)

	if _, ok := om.mq[key]; !ok {
		q := &offsetQueue{offset: offset}
		heap.Push(q, offset)
		om.mq[key] = q
		logp.Info("init topic:%v, partition:%v, offset:%v", topic, partition, offset)
	}
}

func Update(topic string, partition int32, offset int64) {
	logp.Debug("Update", "update topic:%v, partition:%v, offset:%v", topic, partition, offset)

	key := fmt.Sprintf("%v_%v", topic, partition)

	om.Lock()
	defer om.Unlock()

	q, ok := om.mq[key]
	if !ok {
		q = &offsetQueue{}
		om.mq[key] = q
	}

	top := q.Top()
	if top == offset {
		return
	}

	heap.Push(q, offset)

	if top+1 == offset {
		offsetClean(q)
		logp.Debug("Update", "clean topic:%v, partition:%v, offset:%v, array:%v", topic, partition, q.offset, q.queue)
		om.consumer.MarkOffset(&sarama.ConsumerMessage{Topic: topic, Partition: partition, Offset: offset}, "")
		logp.Info("markOffset topic:%v, partition:%v, offset:%v", topic, partition, offset)
	}
}

func status() {
	om.Lock()
	defer om.Unlock()

	for k, v := range om.mq {
		logp.Debug("kafkabeat", "k:%v, offset:%v, queue:%v", k, v.offset, v.queue)
	}
}
