package otel

import (
	context2 "context"
	"errors"
	"fmt"
	"github.com/asynkron/protoactor-go/actor"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"log/slog"
)

func setSpanContextToEnvelope(spanCtx trace.SpanContext, envelope *actor.MessageEnvelope) {
	envelope.SetHeader("parent-id", spanCtx.SpanID().String())
	envelope.SetHeader("trace-id", spanCtx.TraceID().String())
	envelope.SetHeader("tracestate", spanCtx.TraceState().String())
	envelope.SetHeader("trace-flags", fmt.Sprintf("%02x", byte(spanCtx.TraceFlags())))
}

func SpanContextMapFromSpanContext(spanCtx trace.SpanContext) map[string]string {
	return map[string]string{
		"parent-id":   spanCtx.SpanID().String(),
		"trace-id":    spanCtx.TraceID().String(),
		"tracestate":  spanCtx.TraceState().String(),
		"trace-flags": fmt.Sprintf("%02x", byte(spanCtx.TraceFlags())),
	}
}

func extractSpanContextFromSenderFuncArgs(c actor.SenderContext, envelope *actor.MessageEnvelope) (trace.SpanContext, error) {
	fromCtxMessageHeader, err := spanContextFromMessageHeader(c.MessageHeader())
	if !errors.Is(err, ErrSpanContextNotFound) {
		return fromCtxMessageHeader, nil
	}
	return spanContextFromMessageHeader(envelope.Header)
}

func SenderMiddleware() actor.SenderMiddleware {
	return func(next actor.SenderFunc) actor.SenderFunc {
		return func(c actor.SenderContext, target *actor.PID, envelope *actor.MessageEnvelope) {
			ctxWithParentSpan := context2.Background()

			var activeSpan trace.Span
			if receiver, ok := c.(actor.ReceiverContext); ok {
				activeSpan = GetActiveSpan(receiver)
				ctxWithParentSpan = trace.ContextWithSpan(ctxWithParentSpan, activeSpan)
			}

			if activeSpan == nil {
				spanContext, err := extractSpanContextFromSenderFuncArgs(c, envelope)
				if errors.Is(err, ErrSpanContextNotFound) {
					c.Logger().Debug("INBOUND No spanContext found", slog.Any("self", c.Self()), slog.Any("error", err))
				} else if err != nil {
					c.Logger().Error("INBOUND Error extracting spanContext", slog.Any("self", c.Self()), slog.Any("error", err))
				} else {
					ctxWithParentSpan = trace.ContextWithSpanContext(ctxWithParentSpan, spanContext)
				}
			}

			traceExt := c.ActorSystem().Extensions.Get(extensionID).(*TraceExtension)
			ctxWithCurrentSpan, span := traceExt.Tracer().Start(ctxWithParentSpan, fmt.Sprintf("message_send/%T", envelope.Message))
			span.SetAttributes(attribute.String("SenderActorPID", c.Self().String()))
			span.SetAttributes(attribute.String("SenderActorType", fmt.Sprintf("%T", c.Actor())))
			span.SetAttributes(attribute.String("TargetActorPID", target.String()))
			span.SetAttributes(attribute.String("MessageType", fmt.Sprintf("%T", envelope.Message)))
			setSpanContextToEnvelope(trace.SpanContextFromContext(
				ctxWithCurrentSpan,
			), envelope)

			c.Logger().Debug("OUTBOUND Successfully injected", slog.Any("self", c.Self()), slog.Any("actor", c.Actor()), slog.Any("message", envelope.Message))
			next(c, target, envelope)
			span.End()
		}
	}
}
