package otel

import (
	context2 "context"
	"fmt"
	"github.com/asynkron/protoactor-go/actor"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"log/slog"
)

func SpawnMiddleware() actor.SpawnMiddleware {
	return func(next actor.SpawnFunc) actor.SpawnFunc {
		return func(actorSystem *actor.ActorSystem, id string, props *actor.Props, parentContext actor.SpawnerContext) (pid *actor.PID, e error) {
			self := parentContext.Self()
			traceExt := actorSystem.Extensions.Get(extensionID).(*TraceExtension)
			ctxWithParentSpan := context2.Background()
			receiver, ok := parentContext.(actor.ReceiverContext)
			if ok {
				ext := receiver.Get(ctxExtensionID).(*TraceCtxExtension)
				activeSpan, ok := ext.activeSpan.Load(self)
				if ok {
					ctxWithParentSpan = trace.ContextWithSpan(context2.Background(), activeSpan.(trace.Span))
				}
			}
			_, span := traceExt.Tracer().Start(ctxWithParentSpan, fmt.Sprintf("spawn/%s", id))
			span.SetAttributes(attribute.String("ParentActorPID", self.String()))
			span.SetAttributes(attribute.String("SpawnActorPID", pid.String()))
			defer span.End()
			pid, err := next(actorSystem, id, props, parentContext)
			if err != nil {
				span.RecordError(err)
				actorSystem.Logger().Debug("SPAWN got error trying to spawn", slog.Any("self", self), slog.Any("actor", parentContext.Actor()), slog.Any("error", err))
				return pid, err
			}
			return pid, err
		}
	}
}
