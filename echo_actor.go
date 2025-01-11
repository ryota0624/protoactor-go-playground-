package playground

import (
	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/cluster"
	otelmiddleware "github.com/ryota0624/protoactor-go-playground/middleware/otel"
	"github.com/ryota0624/protoactor-go-playground/proto/gen/echo"
	"log"
	"time"
)

var _ echo.Echo = (*EchoActor)(nil)

type EchoActor struct {
}

func (a *EchoActor) Init(ctx cluster.GrainContext) {
	log.Printf("echo actor started: %s\n", ctx.Self())
}

func (a *EchoActor) Terminate(ctx cluster.GrainContext) {
	log.Printf("echo actor stopped: %s\n", ctx.Self())
}

func (a *EchoActor) ReceiveDefault(ctx cluster.GrainContext) {
	a.Receive(ctx)
}

func (a *EchoActor) SayMessage(req *echo.Say, ctx cluster.GrainContext) (*echo.SayResponse, error) {

	helloGrainResponse, err := ctx.Cluster().RequestFuture(ctx.Identity(), echo.GetEchoKind().Kind, req, cluster.WithContext(ctx.ActorSystem().Root.Copy().WithSenderMiddleware(otelmiddleware.RootContextSenderMiddleware())))

	if err != nil {
		return nil, err
	}
	result, err := helloGrainResponse.Result()
	if err != nil {
		return nil, err
	}
	return result.(*echo.SayResponse), nil
}

type Say struct {
	Message string
}

type SayResponse struct {
	Message string
}

func (*EchoActor) Receive(context actor.Context) {
	switch msg := context.Message().(type) {
	case *echo.Say:
		log.Printf("echo actor received: %s\n", msg.Message)
		context.Respond(&echo.SayResponse{Message: msg.Message + "-response"})
	case Say:
		log.Printf("echo actor received: %s %s \n", msg.Message, context.Self())

		if msg.Message == "hello" {
			//go func() {
			<-time.NewTimer(1 * time.Second).C
			pid, err := context.SpawnNamed(actor.PropsFromProducer(func() actor.Actor {
				return &EchoActor{}
			}), "child-echo-actor")
			if err != nil {
				log.Printf("spawn child error: %v\n", err)
			}
			context.Request(pid, Say{Message: "world"})
			//}()
		}
		context.Respond(SayResponse{Message: msg.Message})
	}
}
