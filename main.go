package main

import (
	"context"
	"fmt"
	"github.com/asynkron/protoactor-go/router"
	"github.com/lmittmann/tint"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"log"
	"log/slog"
	"os"
	"time"

	console "github.com/asynkron/goconsole"
	"github.com/asynkron/protoactor-go/actor"
	otelmiddleware "github.com/ryota0624/protoactor-go-playground/middleware/otel"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/stdout/stdoutmetric"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

func SpanAddedRootContext(sys *actor.ActorSystem, span trace.Span) *actor.RootContext {
	return actor.NewRootContext(sys,
		otelmiddleware.SpanContextMapFromSpanContext(span.SpanContext()),
	).WithSenderMiddleware(otelmiddleware.SenderMiddleware()).WithSpawnMiddleware(otelmiddleware.TracingMiddleware(), otelmiddleware.SpawnMiddleware())
}

func main() {
	meterProvider, traceProvider, cleanup := SetUpTelemetry()
	defer func() {
		for _, fn := range cleanup {
			fn()
		}
	}()

	config := actor.Configure(actor.WithMetricProviders(meterProvider), actor.WithLoggerFactory(func(system *actor.ActorSystem) *slog.Logger {
		return createActorSystemLogger(system)
	}))
	sys := actor.NewActorSystemWithConfig(config)
	sys.Extensions.Register(otelmiddleware.NewTraceExtension(traceProvider))
	root := actor.NewRootContext(sys, nil).WithSenderMiddleware(otelmiddleware.SenderMiddleware()).WithSpawnMiddleware(otelmiddleware.TracingMiddleware(), otelmiddleware.SpawnMiddleware())
	_, span := traceProvider.Tracer("echo-actor").Start(context.Background(), "broadcast-echo")

	var echoPids []*actor.PID
	for i := 0; i < 3; i++ {
		echoPid := root.SpawnPrefix(actor.PropsFromProducer(func() actor.Actor {
			return &EchoActor{}
		}), "echo-actor")
		echoPids = append(echoPids, echoPid)
	}

	echoActorBroadcast := root.SpawnPrefix(router.NewBroadcastGroup(echoPids...), "echo-actor-broadcast")

	f := root.Copy().WithHeaders(otelmiddleware.SpanContextMapFromSpanContext(span.SpanContext())).RequestFuture(echoActorBroadcast,
		Say{
			message: "hello",
		}, 3*time.Second)
	success, err := f.Result()
	if err != nil {
		log.Printf("error: %v\n", err)
	} else {
		fmt.Printf("response: %s\n", success.(SayResponse).message)
	}

	f = root.Copy().WithHeaders(otelmiddleware.SpanContextMapFromSpanContext(span.SpanContext())).RequestFuture(echoActorBroadcast,
		Say{
			message: "world",
		}, 3*time.Second)
	success, err = f.Result()
	if err != nil {
		log.Printf("error: %v\n", err)
	} else {
		fmt.Printf("response: %s\n", success.(SayResponse).message)
	}

	span.End()

	_, _ = console.ReadLine()
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

func SetUpTelemetry() (*metric.MeterProvider, trace.TracerProvider, []func()) {

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

	tp := newTraceExporter(err, res)
	cleanup = append(cleanup, func() {
		ctx := context.Background()

		err := tp.Shutdown(ctx)
		if err != nil {
			log.Fatalf("failed to shutdown tracer provider: %v", err)
		}
	})
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	otel.SetMeterProvider(meterProvider)
	return meterProvider, tp, cleanup
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
			metric.NewPeriodicReader(grpc, metric.WithInterval(30*time.Second)),
		),
		metric.WithReader(metric.NewPeriodicReader(metricExporter, metric.WithInterval(
			30*time.Second))),
	)
	return meterProvider, nil
}

func newTraceExporter(err error, res *resource.Resource) *sdktrace.TracerProvider {
	traceExporter, err := otlptracegrpc.New(context.Background(), otlptracegrpc.WithInsecure())
	if err != nil {
		log.Fatalf("failed to create stdout trace exporter: %v", err)
	}

	traceConsoleExporter, err := stdouttrace.New()

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(traceExporter),
		sdktrace.WithBatcher(traceConsoleExporter),
		sdktrace.WithResource(res),
	)

	return tp
}

func createActorSystemLogger(system *actor.ActorSystem) *slog.Logger {
	w := os.Stderr
	// create a new logger
	return slog.New(tint.NewHandler(w, &tint.Options{
		Level:      slog.LevelDebug,
		TimeFormat: time.Kitchen,
	})).With("lib", "Proto.Actor").
		With("system", system.ID)
}
