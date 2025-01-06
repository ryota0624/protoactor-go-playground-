package otel

import (
	context2 "context"
	"encoding/hex"
	"errors"
	"fmt"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"log/slog"

	"github.com/asynkron/protoactor-go/actor"
)

var ErrSpanContextNotFound = fmt.Errorf("spanContext not found")

func spanContextFromMessageHeader(header actor.ReadonlyMessageHeader) (trace.SpanContext, error) {
	if header == nil {
		return trace.SpanContext{}, ErrSpanContextNotFound
	}

	gotSpanId, err := trace.SpanIDFromHex(header.Get("parent-id"))
	if err != nil {
		return trace.SpanContext{}, fmt.Errorf("failed to parse spanId: %v: %w", err, ErrSpanContextNotFound)
	}

	gotTraceId, err := trace.TraceIDFromHex(header.Get("trace-id"))
	if err != nil {
		return trace.SpanContext{}, fmt.Errorf("failed to parse traceId: %v: %w", err, ErrSpanContextNotFound)
	}

	tracestate, err := trace.ParseTraceState(header.Get("tracestate"))
	if err != nil {
		return trace.SpanContext{}, fmt.Errorf("failed to parse tracestate: %v: %w", err, ErrSpanContextNotFound)
	}

	traceFlags, err := hex.DecodeString(header.Get("trace-flags"))
	if err != nil {
		return trace.SpanContext{}, fmt.Errorf("failed to parse traceFlags: %v: %w", err, ErrSpanContextNotFound)
	}

	return trace.NewSpanContext(
		trace.SpanContextConfig{
			SpanID:     gotSpanId,
			TraceID:    gotTraceId,
			TraceState: tracestate,
			TraceFlags: trace.TraceFlags(traceFlags[0]),
		}), nil
}

func ReceiverMiddleware() actor.ReceiverMiddleware {
	return func(next actor.ReceiverFunc) actor.ReceiverFunc {
		return func(c actor.ReceiverContext, envelope *actor.MessageEnvelope) {
			traceExt := c.ActorSystem().Extensions.Get(extensionID).(*TraceExtension)
			var ctxWithSpan context2.Context
			if envelope.Header == nil {
				ctxWithSpan = context2.Background()
			} else {
				spanContext, err := spanContextFromMessageHeader(envelope.Header)
				if errors.Is(err, ErrSpanContextNotFound) {
					c.Logger().Debug("INBOUND No spanContext found", slog.Any("self", c.Self()), slog.Any("error", err))
					ctxWithSpan = context2.Background()
				} else if err != nil {
					c.Logger().Debug("INBOUND Error", slog.Any("self", c.Self()), slog.Any("error", err))
					ctxWithSpan = context2.Background()
				} else {
					ctxWithSpan = trace.ContextWithSpanContext(context2.Background(), spanContext)
				}
			}
			startSpan := func(suffix string) trace.Span {
				_, span := traceExt.Tracer().Start(ctxWithSpan, fmt.Sprintf("message_receive/%T/%s", c.Actor(), suffix))
				span.SetAttributes(attribute.String("ActorPID", c.Self().String()))
				span.SetAttributes(attribute.String("ActorType", fmt.Sprintf("%T", c.Actor())))
				span.SetAttributes(attribute.String("MessageType", fmt.Sprintf("%T", envelope.Message)))
				return span
			}

			switch envelope.Message.(type) {
			case *actor.Started:
				span := startSpan("started")
				next(c, envelope)
				span.End()
				return
			case *actor.Stopping:
				span := startSpan("stopping")
				next(c, envelope)
				span.End()
				return
			case *actor.Stopped:
				span := startSpan("stopped")
				next(c, envelope)
				span.End()
				return
			}

			span := startSpan(fmt.Sprintf("%T", envelope.Message))
			setActiveSpan(c, span)
			defer func() {
				c.Logger().Debug("INBOUND Finishing span", slog.Any("self", c.Self()), slog.Any("actor", c.Actor()), slog.Any("message", envelope.Message))
				span.End()
				clearActiveSpan(c)
			}()

			next(c, envelope)
		}
	}
}
