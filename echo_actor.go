package playground

import (
	"github.com/asynkron/protoactor-go/actor"
	"github.com/ryota0624/protoactor-go-playground/proto/gen/echo"
	"log"
	"time"
)

type EchoActor struct {
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
		context.Respond(&echo.SayResponse{Message: msg.Message})
	case Say:
		if msg.Message == "hello" {
			//go func() {
			<-time.NewTimer(1 * time.Second).C
			pid, err := context.SpawnNamed(actor.PropsFromProducer(func() actor.Actor {
				return &EchoActor{}
			}), "child-echo-actor")
			if err != nil {
				log.Printf("error: %v\n", err)
			}

			context.Request(pid, Say{Message: "world"})
			//}()
		}
		context.Respond(SayResponse{Message: msg.Message})
	}
}
