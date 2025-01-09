package main

import (
	"context"
	"fmt"
	"github.com/asynkron/protoactor-go/router"
	playground "github.com/ryota0624/protoactor-go-playground"
	"log"
	"log/slog"
	"time"

	console "github.com/asynkron/goconsole"
	"github.com/asynkron/protoactor-go/actor"
	otelmiddleware "github.com/ryota0624/protoactor-go-playground/middleware/otel"
)

func main() {
	meterProvider, traceProvider, cleanup := playground.SetUpTelemetry()
	defer func() {
		for _, fn := range cleanup {
			fn()
		}
	}()

	config := actor.Configure(actor.WithMetricProviders(meterProvider), actor.WithLoggerFactory(func(system *actor.ActorSystem) *slog.Logger {
		return playground.CreateActorSystemLogger(system, slog.LevelDebug)
	}))
	sys := actor.NewActorSystemWithConfig(config)
	sys.Extensions.Register(otelmiddleware.NewTraceExtension(traceProvider))
	root := actor.NewRootContext(sys, nil).WithSenderMiddleware(otelmiddleware.SenderMiddleware()).WithSpawnMiddleware(otelmiddleware.TracingMiddleware(), otelmiddleware.SpawnMiddleware())
	_, span := traceProvider.Tracer("echo-actor").Start(context.Background(), "broadcast-echo")

	var echoPids []*actor.PID
	for i := 0; i < 3; i++ {
		echoPid := root.SpawnPrefix(actor.PropsFromProducer(func() actor.Actor {
			return &playground.EchoActor{}
		}), "echo-actor")
		echoPids = append(echoPids, echoPid)
	}

	echoActorBroadcast := root.SpawnPrefix(router.NewBroadcastGroup(echoPids[0]), "echo-actor-broadcast")

	f := root.Copy().WithHeaders(otelmiddleware.SpanContextMapFromSpanContext(span.SpanContext())).RequestFuture(echoActorBroadcast,
		playground.Say{
			Message: "hello",
		}, 3*time.Second)
	success, err := f.Result()
	if err != nil {
		log.Printf("error: %v\n", err)
	} else {
		fmt.Printf("response: %s\n", success.(playground.SayResponse).Message)
	}

	f = root.Copy().WithHeaders(otelmiddleware.SpanContextMapFromSpanContext(span.SpanContext())).RequestFuture(echoActorBroadcast,
		playground.Say{
			Message: "world",
		}, 3*time.Second)
	success, err = f.Result()
	if err != nil {
		log.Printf("error: %v\n", err)
	} else {
		fmt.Printf("response: %s\n", success.(playground.SayResponse).Message)
	}

	//f = root.Copy().WithHeaders(otelmiddleware.SpanContextMapFromSpanContext(span.SpanContext())).RequestFuture(echoActorBroadcast,
	//	Say{
	//		message: "world",
	//	}, 3*time.Second)
	//success, err = f.Result()
	//if err != nil {
	//	log.Printf("error: %v\n", err)
	//} else {
	//	fmt.Printf("response: %s\n", success.(SayResponse).message)
	//}

	span.End()

	_, _ = console.ReadLine()
}
