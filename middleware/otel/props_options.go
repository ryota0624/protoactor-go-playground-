package otel

import (
	"github.com/asynkron/protoactor-go/actor"
)

func TracingPropsOptions() []actor.PropsOption {
	return []actor.PropsOption{
		actor.WithReceiverMiddleware(ReceiverMiddleware()),
		actor.WithSpawnMiddleware(SpawnMiddleware()),
		actor.WithSenderMiddleware(SenderMiddleware()),
	}
}
