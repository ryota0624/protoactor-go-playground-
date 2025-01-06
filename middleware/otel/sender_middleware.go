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

func SenderMiddleware() actor.SenderMiddleware {
	return func(next actor.SenderFunc) actor.SenderFunc {
		return func(c actor.SenderContext, target *actor.PID, envelope *actor.MessageEnvelope) {
			SenderReceiveWithSpanMessageMiddleware()(senderMiddleware()(next))(c, target, envelope)
		}
	}
}

func senderMiddleware() actor.SenderMiddleware {
	return func(next actor.SenderFunc) actor.SenderFunc {
		return func(c actor.SenderContext, target *actor.PID, envelope *actor.MessageEnvelope) {
			c.Logger().Debug("INBOUND senderMiddleware", slog.Any("self", c.Self()), slog.Any("message", envelope.Message))

			ctxWithParentSpan := context2.Background()
			spanContext, err := spanContextFromMessageHeader(envelope.Header)
			if errors.Is(err, ErrSpanContextNotFound) {
				c.Logger().Debug("INBOUND No spanContext found", slog.Any("self", c.Self()), slog.Any("error", err))
			} else {
				ctxWithParentSpan = trace.ContextWithSpanContext(ctxWithParentSpan, spanContext)
			}
			if !spanContext.IsValid() {
				receiver, ok := c.(actor.ReceiverContext)
				if ok {
					ext := receiver.Get(ctxExtensionID).(*TraceCtxExtension)
					activeSpan, ok := ext.activeSpan.Load(c.Self())
					if ok {
						ctxWithParentSpan = trace.ContextWithSpan(ctxWithParentSpan, activeSpan.(trace.Span))
					}
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

type WithSpanMessage[T any] struct {
	Span    trace.Span
	Message T
}

func WrapWithSpanMessage[T any](span trace.Span, message T) WithSpanMessage[any] {
	return WithSpanMessage[any]{
		Span:    span,
		Message: message,
	}
}

func SenderReceiveWithSpanMessageMiddleware() actor.SenderMiddleware {
	return func(next actor.SenderFunc) actor.SenderFunc {
		return func(c actor.SenderContext, target *actor.PID, envelope *actor.MessageEnvelope) {
			c.Logger().Debug("INBOUND SenderReceiveWithSpanMessageMiddleware", slog.Any("self", c.Self()))

			if withSpanMessage, ok := envelope.Message.(WithSpanMessage[any]); ok {
				c.Logger().Debug("INBOUND WithSpanMessage", slog.Any("self", c.Self()))
				envelope.Message = withSpanMessage.Message
				setSpanContextToEnvelope(withSpanMessage.Span.SpanContext(), envelope)
			} else {
				c.Logger().Debug("INBOUND No WithSpanMessage", slog.Any("self", c.Self()))
			}
			next(c, target, envelope)
		}
	}
}
