package otel

import (
	"github.com/asynkron/protoactor-go/ctxext"
	"sync"
)

var ctxExtensionID = ctxext.NextContextExtensionID()

type TraceCtxExtension struct {
	activeSpan sync.Map
}

func (t *TraceCtxExtension) ExtensionID() ctxext.ContextExtensionID {
	return ctxExtensionID
}

var _ ctxext.ContextExtension = &TraceCtxExtension{}
