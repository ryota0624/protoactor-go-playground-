package playground

import (
	"context"
	"github.com/asynkron/protoactor-go/actor"
	"github.com/lmittmann/tint"
	otelmiddleware "github.com/ryota0624/protoactor-go-playground/middleware/otel"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/stdout/stdoutmetric"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
	"log"
	"log/slog"
	"math/rand"
	"os"
	"strconv"
	"time"
)

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
			semconv.ServiceInstanceID(strconv.Itoa(rand.Int())),
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

func CreateActorSystemLogger(system *actor.ActorSystem, level slog.Level) *slog.Logger {
	w := os.Stderr
	// create a new logger
	return slog.New(tint.NewHandler(w, &tint.Options{
		Level:      level,
		TimeFormat: time.Kitchen,
	})).With("lib", "Proto.Actor").
		With("system", system.ID)
}

func SpanAddedRootContext(sys *actor.ActorSystem, span trace.Span) *actor.RootContext {
	return actor.NewRootContext(sys,
		otelmiddleware.SpanContextMapFromSpanContext(span.SpanContext()),
	).WithSenderMiddleware(otelmiddleware.SenderMiddleware()).WithSpawnMiddleware(otelmiddleware.TracingMiddleware(), otelmiddleware.SpawnMiddleware())
}
