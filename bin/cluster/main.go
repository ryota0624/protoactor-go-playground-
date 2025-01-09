package main

import (
	"context"
	console "github.com/asynkron/goconsole"
	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/cluster"
	"github.com/asynkron/protoactor-go/cluster/clusterproviders/automanaged"
	"github.com/asynkron/protoactor-go/remote"

	"github.com/asynkron/protoactor-go/cluster/identitylookup/disthash"
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
	echo.EchoFactory(func() echo.Echo {
		log.Printf("echo actor created\n")
		return &playground.EchoActor{}
	})

	meterProvider, traceProvider, cleanup := playground.SetUpTelemetry()
	defer func() {
		for _, fn := range cleanup {
			fn()
		}
	}()

	config := actor.Configure(actor.WithMetricProviders(meterProvider), actor.WithLoggerFactory(func(system *actor.ActorSystem) *slog.Logger {
		return playground.CreateActorSystemLogger(system, slog.LevelInfo)
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

	clusterConfig := cluster.Configure("echo-cluster", automanaged.NewWithConfig(5*time.Second,
		port+100, "127.0.0.1:9101", "127.0.0.1:8989",
	), disthash.New(), remoteConfig, cluster.WithKinds(echo.GetEchoKind()))
	c := cluster.New(sys, clusterConfig)
	c.StartMember()
	defer c.Shutdown(true)

	for {
		if c.MemberList.Length() > 1 {

			break
		}
		log.Printf("waiting for cluster to form...\n")
		time.Sleep(1 * time.Second)
	}

	echoClient := echo.GetEchoGrainClient(c, "echo-actor-grain-1")
	echoGrainResponse, err := echoClient.SayMessage(&echo.Say{
		Message: "hello grain1",
	})

	if err != nil {
		log.Printf("error: %v\n", err)
	} else {
		log.Printf("response from echo-actor-grain-1: %v\n", echoGrainResponse.Message)
	}

	helloGrainResponse, err := c.RequestFuture("echo-actor-grain-2", echo.GetEchoKind().Kind, &echo.Say{
		Message: "hello grain2",
	})

	if err != nil {
		log.Printf("error: %v\n", err)
	} else {
		response, err := helloGrainResponse.Result()
		if err != nil {
			log.Printf("error: %v\n", err)
		} else {
			log.Printf("response from echo-actor-grain-2: %v\n", response)
		}
	}

	rootContext := actor.NewRootContext(sys, nil).WithSenderMiddleware(otelmiddleware.SenderMiddleware()).WithSpawnMiddleware(otelmiddleware.TracingMiddleware(), otelmiddleware.SpawnMiddleware())
	_, err = c.Remote.SpawnNamed("127.0.0.1:8889", "echo-actor-1", "echo-actor", 5*time.Second)
	if err != nil {
		log.Printf("error: %v\n", err)
	}

	echoActorPidResponse, err := rootContext.Copy().WithHeaders(
		otelmiddleware.SpanContextMapFromSpanContext(span.SpanContext()),
	).RequestFuture(c.Remote.ActivatorForAddress("127.0.0.1:8889"), &remote.ActorPidRequest{
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
