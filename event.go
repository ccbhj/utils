package aego

import (
	"context"
	"time"

	"github.com/pkg/errors"
)

type AeTimeEvCB func(ctx context.Context, id int64) (againAfter time.Duration)
type AeTimeEvFiniCB func(ctx context.Context)

type AeRequest func() interface{}
type AeIOCB func(interface{})

var ioEvId int64 = 0

var ErrBusy error = errors.New("AE busy")

type AeTimeEv struct {
	id       int64
	whenTime time.Time
	cb       AeTimeEvCB
	fcb      AeTimeEvFiniCB
	ctx      context.Context
	prev     *AeTimeEv
	next     *AeTimeEv
	refcount int
}

type AeIOEv struct {
	// AERequest or AEResponse
	// mask   int32
	sendCB AeRequest
	doneCB AeIOCB
	err    error
	ctx    context.Context
}

func newAeTimeEv(id int64, when time.Time, cb AeTimeEvCB, fcb AeTimeEvFiniCB, ctx context.Context) *AeTimeEv {
	return &AeTimeEv{
		id:       id,
		whenTime: when,
		cb:       cb,
		fcb:      fcb,
		ctx:      ctx,
	}
}

func newIOEv(ctx context.Context, sendCB AeRequest, cb AeIOCB) *AeIOEv {
	return &AeIOEv{
		sendCB: sendCB,
		doneCB: cb,
		ctx:    ctx,
		err:    nil,
	}
}
