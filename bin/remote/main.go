package main

import (
	"context"
	console "github.com/asynkron/goconsole"
	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/remote"
	playground "github.com/ryota0624/protoactor-go-playground"
	otelmiddleware "github.com/ryota0624/protoactor-go-playground/middleware/otel"
	"github.com/ryota0624/protoactor-go-playground/proto/gen/echo"
	"go.opentelemetry.io/otel"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
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
				actor.WithContextDecorator(otelmiddleware.ContextDecorator()),
			),
		}),
	)
	rt := remote.NewRemote(sys, remoteConfig)
	rt.Start()
	defer rt.Shutdown(true)

	rootContext := actor.NewRootContext(sys, nil).WithSenderMiddleware(otelmiddleware.RootContextSenderMiddleware()).WithSpawnMiddleware(otelmiddleware.RootContextSpawnMiddleware())
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
