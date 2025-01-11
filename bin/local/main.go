package main

import (
	"context"
	"fmt"
	"github.com/asynkron/protoactor-go/router"
	playground "github.com/ryota0624/protoactor-go-playground"
	"go.opentelemetry.io/otel"
	"log"
	"log/slog"
	"time"

	console "github.com/asynkron/goconsole"
	"github.com/asynkron/protoactor-go/actor"
	otelmiddleware "github.com/ryota0624/protoactor-go-playground/middleware/otel"
)

func main() {
	resource, err := playground.NewResource("sample-app", "0.1.0", "TODO")
	if err != nil {
		log.Fatalf("failed to create resource: %v", err)
	}
	meterProvider, traceProvider := playground.SetUpTelemetry(resource)
	defer func() {
		err := meterProvider.Shutdown(context.Background())
		if err != nil {
			log.Printf("failed to shutdown meter provider: %v\n", err)
		}
		err = traceProvider.Shutdown(context.Background())
		if err != nil {
			log.Printf("failed to shutdown trace provider: %v\n", err)
		}
	}()

	config := actor.Configure(actor.WithMetricProviders(meterProvider), actor.WithLoggerFactory(func(system *actor.ActorSystem) *slog.Logger {
		return playground.CreateActorSystemLogger(system, slog.LevelDebug)
	}))

	sys := actor.NewActorSystemWithConfig(config)

	sys.Extensions.Register(otelmiddleware.NewTraceExtension(traceProvider))
	otel.SetTracerProvider(traceProvider)
	otel.SetMeterProvider(meterProvider)

	root := actor.NewRootContext(sys, nil).WithSenderMiddleware(otelmiddleware.RootContextSenderMiddleware()).WithSpawnMiddleware(otelmiddleware.RootContextSpawnMiddleware())

	_, span := traceProvider.Tracer("echo-actor").Start(context.Background(), "broadcast-echo")
	var echoPids []*actor.PID
	for i := 0; i < 3; i++ {
		echoPid := root.Copy().WithHeaders(otelmiddleware.SpanContextMapFromSpanContext(span.SpanContext())).SpawnPrefix(actor.PropsFromProducer(func() actor.Actor {
			return &playground.EchoActor{}
		}), "echo-actor")
		echoPids = append(echoPids, echoPid)
	}

	echoActorBroadcast := root.Copy().WithHeaders(otelmiddleware.SpanContextMapFromSpanContext(span.SpanContext())).SpawnPrefix(router.NewBroadcastGroup(echoPids[0]), "echo-actor-broadcast")

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

	f = root.RequestFuture(echoActorBroadcast,
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
