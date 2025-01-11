package main

import (
	"context"
	console "github.com/asynkron/goconsole"
	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/cluster"
	"github.com/asynkron/protoactor-go/cluster/clusterproviders/automanaged"
	"github.com/asynkron/protoactor-go/cluster/identitylookup/disthash"
	"github.com/asynkron/protoactor-go/remote"
	playground "github.com/ryota0624/protoactor-go-playground"
	otelmiddleware "github.com/ryota0624/protoactor-go-playground/middleware/otel"
	"github.com/ryota0624/protoactor-go-playground/proto/gen/echo"
	"go.opentelemetry.io/otel"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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

	config := actor.Configure(actor.WithMetricProviders(nil), actor.WithLoggerFactory(func(system *actor.ActorSystem) *slog.Logger {
		return playground.CreateActorSystemLogger(system, slog.LevelInfo)
	}))
	sys := actor.NewActorSystemWithConfig(config)
	resource, err := playground.NewResource("sample-app", "0.1.0", sys.ID)
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

	sys.Extensions.Register(otelmiddleware.NewTraceExtension(traceProvider))
	otel.SetTracerProvider(traceProvider)
	otel.SetMeterProvider(meterProvider)

	_, span := traceProvider.Tracer("echo-actor").Start(context.Background(), "remote-echo")
	defer span.End()

	port, err := strconv.Atoi(os.Getenv("PORT"))
	if err != nil {
		port = 8889
	}

	remoteConfig := remote.Configure("127.0.0.1", port, remote.WithDialOptions(
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	))
	autoManagedProvider := automanaged.NewWithConfig(5*time.Second,
		port+100, "127.0.0.1:9101", "127.0.0.1:8989",
	)
	clusterConfig := cluster.Configure("echo-cluster", autoManagedProvider, disthash.New(), remoteConfig, cluster.WithKinds(echo.GetEchoKind(actor.WithContextDecorator(otelmiddleware.ContextDecorator()))))
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
	}, cluster.WithContext(sys.Root.Copy().WithSenderMiddleware(otelmiddleware.RootContextSenderMiddleware()).WithHeaders(otelmiddleware.SpanContextMapFromSpanContext(span.SpanContext()))))

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
			helloGrainResponse, err := c.RequestFuture(fromConsole, echo.GetEchoKind().Kind, &echo.Say{
				Message: fromConsole,
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

		}
	}
}
