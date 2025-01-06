package otel

import (
	"sync"

	"github.com/asynkron/protoactor-go/actor"
)
import "go.opentelemetry.io/otel/trace"

func getActiveSpan(context actor.ReceiverContext) trace.Span {
	ctxExt, ok := context.Get(ctxExtensionID).(*TraceCtxExtension)
	if ctxExt == nil || !ok {
		return nil
	}

	value, ok := ctxExt.activeSpan.Load(context.Self())
	if !ok {
		return nil
	}

	span, _ := value.(trace.Span)

	return span
}

func setActiveSpan(context actor.ReceiverContext, span trace.Span) {
	ctxExt, ok := context.Get(ctxExtensionID).(*TraceCtxExtension)
	if !ok || ctxExt == nil {
		ctxExt = &TraceCtxExtension{
			activeSpan: sync.Map{},
		}
		context.Set(ctxExt)
	}

	ctxExt.activeSpan.Store(context.Self(), span)
}

func clearActiveSpan(context actor.ReceiverContext) {
	ctxExt, ok := context.Get(ctxExtensionID).(*TraceCtxExtension)
	if ctxExt == nil || !ok {
		return
	}
	ctxExt.activeSpan.Delete(context.Self())
}

func GetActiveSpan(context actor.ReceiverContext) trace.Span {
	return getActiveSpan(context)
}
