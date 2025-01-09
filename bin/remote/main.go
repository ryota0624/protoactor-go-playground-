package main

import (
	"context"
	console "github.com/asynkron/goconsole"
	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/remote"
	playground "github.com/ryota0624/protoactor-go-playground"
	otelmiddleware "github.com/ryota0624/protoactor-go-playground/middleware/otel"
	"github.com/ryota0624/protoactor-go-playground/proto/gen/echo"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

func main() {
	meterProvider, traceProvider, cleanup := playground.SetUpTelemetry()
	defer func() {
		for _, fn := range cleanup {
			fn()
		}
	}()

	config := actor.Configure(actor.WithMetricProviders(meterProvider), actor.WithLoggerFactory(func(system *actor.ActorSystem) *slog.Logger {
		return playground.CreateActorSystemLogger(system)
	}))
	sys := actor.NewActorSystemWithConfig(config)
	sys.Extensions.Register(otelmiddleware.NewTraceExtension(traceProvider))

	_, span := traceProvider.Tracer("echo-actor").Start(context.Background(), "remote-echo")
	defer span.End()

	port, err := strconv.Atoi(os.Getenv("PORT"))
	if err != nil {
		port = 8889
	}
	remoteConfig := remote.Configure("127.0.0.1", port,
		remote.WithKinds(&remote.Kind{
			Kind: "echo-actor",
			Props: actor.PropsFromProducer(func() actor.Actor {
				return &playground.EchoActor{}
			}).Configure(
				otelmiddleware.TracingPropsOptions()...,
			),
		}),
	)
	rt := remote.NewRemote(sys, remoteConfig)
	rt.Start()
	defer rt.Shutdown(true)

	rootContext := actor.NewRootContext(sys, nil).WithSenderMiddleware(otelmiddleware.SenderMiddleware()).WithSpawnMiddleware(otelmiddleware.TracingMiddleware(), otelmiddleware.SpawnMiddleware())
	_, err = rt.SpawnNamed("127.0.0.1:8889", "echo-actor-1", "echo-actor", 5*time.Second)
	if err != nil {
		log.Printf("error: %v\n", err)
	}

	echoActorPidResponse, err := rootContext.Copy().WithHeaders(
		otelmiddleware.SpanContextMapFromSpanContext(span.SpanContext()),
	).RequestFuture(rt.ActivatorForAddress("127.0.0.1:8889"), &remote.ActorPidRequest{
		Kind: "echo-actor",
		Name: "echo-actor-1",
	}, 5*time.Second).Result()
	if err != nil {
		log.Printf("error: %v\n", err)
	}

	signalTrap := make(chan os.Signal, 1)
	signal.Notify(signalTrap, syscall.SIGTERM, syscall.SIGHUP, syscall.SIGINT)
	consoleTrap := make(chan string)
	go func() {
		for {
			str, _ := console.ReadLine()
			consoleTrap <- str
		}
	}()

	for {
		select {
		case <-signalTrap:
			return
		case fromConsole := <-consoleTrap:
			if response, ok := echoActorPidResponse.(*remote.ActorPidResponse); ok {
				if response, err := rootContext.Copy().WithHeaders(
					otelmiddleware.SpanContextMapFromSpanContext(span.SpanContext()),
				).RequestFuture(response.Pid, &echo.Say{
					Message: fromConsole,
				}, 3*time.Second).Result(); err != nil {
					log.Printf("error: %v\n", err)
				} else {
					log.Printf("response: %v\n", response)
				}
			}
		}
	}

}
