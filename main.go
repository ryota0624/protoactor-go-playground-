package main

import (
	"context"
	"fmt"
	"log"
	"time"

	console "github.com/asynkron/goconsole"
	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/actor/middleware/opentracing"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/stdout/stdoutmetric"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

func main() {
	meterProvider, cleanup := SetUpMeterProvider()
	defer func() {
		for _, fn := range cleanup {
			fn()
		}
	}()
	config := actor.Configure(actor.WithMetricProviders(meterProvider))
	sys := actor.NewActorSystemWithConfig(config)
	root := actor.NewRootContext(sys, nil).WithSpawnMiddleware(opentracing.TracingMiddleware())
	echoPid := root.Spawn(actor.PropsFromProducer(func() actor.Actor {
		return &EchoActor{}
	}))

	f := root.RequestFuture(echoPid, Say{
		message: "hello",
	}, 3*time.Second)
	success, err := f.Result()
	if err != nil {
		log.Printf("error: %v\n", err)
	} else {
		fmt.Printf("response: %s\n", success.(SayResponse).message)
	}

	ticker := time.NewTicker(1 * time.Second)

	go func() {
		for range ticker.C {
			root.Spawn(actor.PropsFromProducer(func() actor.Actor {
				return &EchoActor{}
			}))
		}
	}()

	_, _ = console.ReadLine()
	ticker.Stop()
}

type EchoActor struct {
}

type Say struct {
	message string
}

type SayResponse struct {
	message string
}

func (*EchoActor) Receive(context actor.Context) {
	switch msg := context.Message().(type) {
	case Say:
		context.Respond(SayResponse{message: msg.message})
	}
}

func SetUpMeterProvider() (*metric.MeterProvider, []func()) {

	res, err := newResource("sample-app", "0.1.0")
	if err != nil {
		log.Fatalf("failed to create resource: %v", err)
	}

	meterProvider, err := newMeterProvider(res)
	if err != nil {
		log.Fatalf("failed to create meter provider: %v", err)
	}

	var cleanup []func()

	cleanup = append(cleanup, func() {
		ctx := context.Background()

		err := meterProvider.Shutdown(ctx)
		if err != nil {
			log.Fatalf("failed to shutdown meter provider: %v", err)
		}
	})

	otel.SetMeterProvider(meterProvider)
	return meterProvider, cleanup
}

func newResource(serviceName, serviceVersion string) (*resource.Resource, error) {
	return resource.Merge(resource.Default(),
		resource.NewWithAttributes(semconv.SchemaURL,
			semconv.ServiceName(serviceName),
			semconv.ServiceVersion(serviceVersion),
		))
}

func newMeterProvider(res *resource.Resource) (*metric.MeterProvider, error) {
	metricExporter, err := stdoutmetric.New()
	if err != nil {
		return nil, err
	}

	grpc, err := otlpmetricgrpc.New(context.Background(),
		otlpmetricgrpc.WithInsecure(),
	)

	if err != nil {
		log.Fatalf("failed to create otlp metric exporter: %v", err)
	}

	meterProvider := metric.NewMeterProvider(
		metric.WithResource(res),
		metric.WithReader(
			metric.NewPeriodicReader(grpc, metric.WithInterval(2*time.Second)),
		),
		metric.WithReader(metric.NewPeriodicReader(metricExporter, metric.WithInterval(
			2*time.Second))),
	)
	return meterProvider, nil
}
