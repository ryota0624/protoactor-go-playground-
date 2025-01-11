package otel

import (
	context2 "context"
	"errors"
	"fmt"
	"github.com/asynkron/protoactor-go/actor"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"log/slog"
	"time"
)

type TracingActorContext struct {
	actor.Context
	processPerMessageContext context2.Context
}

var _ actor.Context = (*TracingActorContext)(nil)

func (t *TracingActorContext) Receive(envelope *actor.MessageEnvelope) {
	t.Logger().Debug("Received message", slog.Any("self", t.Self()), slog.Any("message", envelope.Message))
	traceExt := t.ActorSystem().Extensions.Get(extensionID).(*TraceExtension)
	t.processPerMessageContext = context2.Background()
	defer func() {
		t.processPerMessageContext = nil
	}()
	if envelope.Header != nil {
		spanContext, err := spanContextFromMessageHeader(envelope.Header)
		if errors.Is(err, ErrSpanContextNotFound) {
			t.Logger().Debug("INBOUND No spanContext found", slog.Any("self", t.Self()), slog.Any("error", err))
		} else if err != nil {
			t.Logger().Debug("INBOUND Error extracting spanContext", slog.Any("self", t.Self()), slog.Any("error", err))
		} else {
			t.processPerMessageContext = trace.ContextWithSpanContext(t.processPerMessageContext, spanContext)
		}
	}
	startSpan := func(suffix string) trace.Span {
		ctx, span := traceExt.Tracer().Start(t.processPerMessageContext, fmt.Sprintf("message_receive/%T/%s", t.Actor(), suffix))
		span.SetAttributes(attribute.String("ActorPID", t.Self().String()))
		span.SetAttributes(attribute.String("ActorType", fmt.Sprintf("%T", t.Actor())))
		span.SetAttributes(attribute.String("MessageType", fmt.Sprintf("%T", envelope.Message)))
		t.processPerMessageContext = ctx
		return span
	}

	switch envelope.Message.(type) {
	case *actor.Started:
		span := startSpan("started")
		defer span.End()
		return
	case *actor.Stopping:
		span := startSpan("stopping")
		defer span.End()
		return
	case *actor.Stopped:
		span := startSpan("stopped")
		defer span.End()
		return
	default:
		span := startSpan(fmt.Sprintf("%T", envelope.Message))
		defer span.End()
	}

	t.Context.Receive(envelope)
}

func (t *TracingActorContext) Send(pid *actor.PID, message interface{}) {
	traceExt := t.ActorSystem().Extensions.Get(extensionID).(*TraceExtension)
	_, span := traceExt.Tracer().Start(t.processPerMessageContext, fmt.Sprintf("message_send/%T", message))
	defer span.End()
	setSenderSpanAttributes(pid, message, span, t)
	envelop := messageToEnvelop(message, t, t.Self())

	t.Context.Send(pid, envelop)
}

func (t *TracingActorContext) Request(pid *actor.PID, message interface{}) {
	traceExt := t.ActorSystem().Extensions.Get(extensionID).(*TraceExtension)
	_, span := traceExt.Tracer().Start(t.processPerMessageContext, fmt.Sprintf("message_request/%T", message))
	defer span.End()
	setSenderSpanAttributes(pid, message, span, t)
	envelop := messageToEnvelop(message, t, t.Self())

	t.Context.Send(pid, envelop)
}

func (t *TracingActorContext) RequestWithCustomSender(pid *actor.PID, message interface{}, sender *actor.PID) {
	traceExt := t.ActorSystem().Extensions.Get(extensionID).(*TraceExtension)
	_, span := traceExt.Tracer().Start(t.processPerMessageContext, fmt.Sprintf("message_request_with_custom_sender/%T", message))
	defer span.End()
	setSenderSpanAttributes(pid, message, span, t)
	span.SetAttributes(attribute.String("CustomSenderActorPID", sender.String()))

	envelop := messageToEnvelop(message, t, sender)

	t.Context.Send(pid, envelop)
}

func messageToEnvelop(message interface{}, t *TracingActorContext, sender *actor.PID) *actor.MessageEnvelope {
	envelop, ok := message.(*actor.MessageEnvelope)
	if ok {
		setSpanContextToEnvelope(trace.SpanContextFromContext(t.processPerMessageContext), envelop)
	} else {
		envelop = wrapEnvelopeWithSpanContext(trace.SpanContextFromContext(t.processPerMessageContext), message, sender)
	}
	return envelop
}

func (t *TracingActorContext) RequestFuture(pid *actor.PID, message interface{}, timeout time.Duration) *actor.Future {
	future := actor.NewFuture(t.ActorSystem(), timeout)

	traceExt := t.ActorSystem().Extensions.Get(extensionID).(*TraceExtension)
	_, span := traceExt.Tracer().Start(t.processPerMessageContext, fmt.Sprintf("message_request_future/%T", message))
	defer span.End()
	setSenderSpanAttributes(pid, message, span, t)
	envelop := messageToEnvelop(message, t, future.PID())

	t.Context.RequestFuture(pid, envelop, timeout)
	return future

}
func (t *TracingActorContext) Respond(response interface{}) {
	traceExt := t.ActorSystem().Extensions.Get(extensionID).(*TraceExtension)
	_, span := traceExt.Tracer().Start(t.processPerMessageContext, fmt.Sprintf("message_respond/%T", response))
	defer span.End()
	setSenderSpanAttributes(t.Sender(), response, span, t)
	envelop := messageToEnvelop(response, t, t.Self())

	t.Context.Respond(envelop)
}

func ContextDecorator() func(next actor.ContextDecoratorFunc) actor.ContextDecoratorFunc {
	return func(next actor.ContextDecoratorFunc) actor.ContextDecoratorFunc {
		return func(ctx actor.Context) actor.Context {
			return next(&TracingActorContext{ctx, nil})
		}
	}
}

func (t *TracingActorContext) Spawn(props *actor.Props) *actor.PID {
	traceExt := t.ActorSystem().Extensions.Get(extensionID).(*TraceExtension)
	_, span := traceExt.Tracer().Start(t.processPerMessageContext, "spawn")
	defer span.End()
	props.Configure(actor.WithContextDecorator(ContextDecorator()))
	pid := t.Context.Spawn(props)
	span.SetName(fmt.Sprintf("spawn/%s", pid.Id))
	span.SetAttributes(attribute.String("SpawnActorPID", pid.String()))
	return pid
}

func (t *TracingActorContext) SpawnPrefix(props *actor.Props, prefix string) *actor.PID {
	traceExt := t.ActorSystem().Extensions.Get(extensionID).(*TraceExtension)
	_, span := traceExt.Tracer().Start(t.processPerMessageContext, "spawn")
	defer span.End()
	props.Configure(actor.WithContextDecorator(ContextDecorator()))
	pid := t.Context.SpawnPrefix(props, prefix)
	span.SetAttributes(attribute.String("Prefix", prefix))

	span.SetName(fmt.Sprintf("spawn/%s", pid.Id))
	span.SetAttributes(attribute.String("SpawnActorPID", pid.String()))
	return pid
}

func (t *TracingActorContext) SpawnNamed(props *actor.Props, id string) (*actor.PID, error) {
	traceExt := t.ActorSystem().Extensions.Get(extensionID).(*TraceExtension)
	_, span := traceExt.Tracer().Start(t.processPerMessageContext, "spawn")
	defer span.End()
	props.Configure(actor.WithContextDecorator(ContextDecorator()))
	span.SetAttributes(attribute.String("ID", id))
	pid, err := t.Context.SpawnNamed(props, id)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	} else {
		span.SetName(fmt.Sprintf("spawn/%s", pid.Id))
		span.SetAttributes(attribute.String("SpawnActorPID", pid.String()))
	}
	return pid, err
}
