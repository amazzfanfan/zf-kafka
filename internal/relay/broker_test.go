package relay

import (
	"context"
	"encoding/json"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"os"
	"testing"
	"time"
)

func TestKafkaIndependentConsumption(t *testing.T) {
	broker := os.Getenv("TEST_KAFKA_BROKER")
	if broker == "" {
		t.Skip("TEST_KAFKA_BROKER not set")
	}
	s := testStore(t)
	ep := testEndpoint("https://example.com")
	src, r := setupRoute(t, s, ep)
	topic := "relay_test_" + ID()
	src.Type = "kafka"
	src.Brokers = []string{broker}
	src.Topics = []string{topic}
	src.Group = "relay.test." + ID()
	src.Start = "earliest"
	if e := src.Validate(); e != nil {
		t.Fatal(e)
	}
	if e := s.Save(context.Background(), "sources", src.ID, src); e != nil {
		t.Fatal(e)
	}
	_ = r
	admin, e := kgo.NewClient(kgo.SeedBrokers(broker))
	if e != nil {
		t.Fatal(e)
	}
	defer admin.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	create := kmsg.NewPtrCreateTopicsRequest()
	create.Topics = []kmsg.CreateTopicsRequestTopic{{Topic: topic, NumPartitions: 1, ReplicationFactor: 1}}
	reply, e := admin.Request(ctx, create)
	if e != nil {
		t.Fatal(e)
	}
	if reply.(*kmsg.CreateTopicsResponse).Topics[0].ErrorCode != 0 {
		t.Fatal("topic creation rejected")
	}
	defer func() {
		del := kmsg.NewPtrDeleteTopicsRequest()
		del.TopicNames = []string{topic}
		_, _ = admin.Request(context.Background(), del)
	}()
	business, e := kgo.NewClient(kgo.SeedBrokers(broker), kgo.ConsumerGroup("business-test-"+ID()), kgo.ConsumeTopics(topic), kgo.ConsumeStartOffset(kgo.NewOffset().AtStart()), kgo.DisableAutoCommit())
	if e != nil {
		t.Fatal(e)
	}
	defer business.Close()
	m := NewSources(s)
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- m.kafka(runCtx, src) }()
	defer func() { stop(); <-done }()
	records := []*kgo.Record{}
	for i := 0; i < 5; i++ {
		records = append(records, &kgo.Record{Topic: topic, Value: []byte(fmt.Sprintf(`{"n":%d}`, i))})
	}
	if e = admin.ProduceSync(ctx, records...).FirstErr(); e != nil {
		t.Fatal(e)
	}
	businessCount := 0
	for businessCount < 5 {
		fetch := business.PollRecords(ctx, 5)
		if len(fetch.Errors()) > 0 {
			t.Fatal(fetch.Errors())
		}
		businessCount += len(fetch.Records())
		if ctx.Err() != nil {
			t.Fatal("business consumer timed out")
		}
	}
	for {
		var n int
		if e = s.DB.QueryRow(ctx, "SELECT count(*) FROM events WHERE source_id=$1", src.ID).Scan(&n); e != nil {
			t.Fatal(e)
		}
		if n == 5 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("platform did not independently receive all records")
		case <-time.After(100 * time.Millisecond):
		}
	}
}
func TestRabbitIndependentQueue(t *testing.T) {
	address := os.Getenv("TEST_AMQP_URL")
	if address == "" {
		t.Skip("TEST_AMQP_URL not set")
	}
	s := testStore(t)
	ep := testEndpoint("https://example.com")
	src, _ := setupRoute(t, s, ep)
	conn, e := amqp.Dial(address)
	if e != nil {
		t.Fatal(e)
	}
	defer conn.Close()
	ch, e := conn.Channel()
	if e != nil {
		t.Fatal(e)
	}
	defer ch.Close()
	exchange := "test." + ID()
	business := "business." + ID()
	queue := "relay." + ID()
	if e = ch.ExchangeDeclare(exchange, "fanout", true, false, false, false, nil); e != nil {
		t.Fatal(e)
	}
	defer ch.ExchangeDelete(exchange, false, false)
	if _, e = ch.QueueDeclare(business, true, false, false, false, nil); e != nil {
		t.Fatal(e)
	}
	defer ch.QueueDelete(business, false, false, false)
	if e = ch.QueueBind(business, "", exchange, false, nil); e != nil {
		t.Fatal(e)
	}
	if _, e = ch.QueueDeclare(queue, true, false, false, false, amqp.Table{"x-queue-type": "quorum", "x-max-length-bytes": int64(256 * 1048576), "x-overflow": "reject-publish"}); e != nil {
		t.Fatal(e)
	}
	defer ch.QueueDelete(queue, false, false, false)
	if e = ch.QueueBind(queue, "", exchange, false, nil); e != nil {
		t.Fatal(e)
	}
	src.Type = "rabbitmq"
	src.URL = address
	src.Queue = queue
	src.Exchange = exchange
	src.Declare = true
	src.Overflow = "reject-publish"
	if e = src.Validate(); e != nil {
		t.Fatal(e)
	}
	if e = s.Save(context.Background(), "sources", src.ID, src); e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	m := NewSources(s)
	done := make(chan error, 1)
	go func() { done <- m.rabbit(ctx, src) }()
	defer func() { cancel(); <-done }()
	for i := 0; i < 5; i++ {
		body, _ := json.Marshal(map[string]int{"n": i})
		if e = ch.PublishWithContext(ctx, exchange, "", false, false, amqp.Publishing{Body: body, DeliveryMode: amqp.Persistent}); e != nil {
			t.Fatal(e)
		}
	}
	for {
		var n int
		if e = s.DB.QueryRow(ctx, "SELECT count(*) FROM events WHERE source_id=$1", src.ID).Scan(&n); e != nil {
			t.Fatal(e)
		}
		if n == 5 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("platform copy missing")
		case <-time.After(100 * time.Millisecond):
		}
	}
	for i := 0; i < 5; i++ {
		msg, ok, e := ch.Get(business, true)
		if e != nil || !ok {
			t.Fatalf("original business message stolen: %v", e)
		}
		if len(msg.Body) == 0 {
			t.Fatal("empty business message")
		}
	}
}
