package otel

import (
	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/actor/middleware/propagator"
)

func RootContextSpawnMiddleware() actor.SpawnMiddleware {
	return propagator.New().
		WithItselfForwarded().
		WithSpawnMiddleware(rootActorSpawnMiddleware()).
		WithContextDecorator(ContextDecorator()).
		SpawnMiddleware
}
