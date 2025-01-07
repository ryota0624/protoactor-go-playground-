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
			ctxWithSpan := context2.Background()
			if envelope.Header != nil {
				spanContext, err := spanContextFromMessageHeader(envelope.Header)
				if errors.Is(err, ErrSpanContextNotFound) {
					c.Logger().Debug("INBOUND No spanContext found", slog.Any("self", c.Self()), slog.Any("error", err))
				} else if err != nil {
					c.Logger().Debug("INBOUND Error extracting spanContext", slog.Any("self", c.Self()), slog.Any("error", err))
				} else {
					ctxWithSpan = trace.ContextWithSpanContext(ctxWithSpan, spanContext)
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
				defer span.End()
				next(c, envelope)
				return
			case *actor.Stopping:
				span := startSpan("stopping")
				defer span.End()
				next(c, envelope)
				span.End()
				return
			case *actor.Stopped:
				span := startSpan("stopped")
				defer span.End()
				next(c, envelope)
				return
			}

			span := startSpan(fmt.Sprintf("%T", envelope.Message))
			defer func() {
				span.End()
				clearActiveSpan(c)
			}()
			setActiveSpan(c, span)

			next(c, envelope)
		}
	}
}
