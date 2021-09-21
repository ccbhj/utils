package aego

import (
	"context"
	"time"

	"github.com/pkg/errors"
)

var ErrEventNotFound = errors.New("Event not found")

const (
	TimeEvDeleted = -1
)

type Flag uint32

const (
	AETimeEv         = 1 << 0
	AEIOEv           = 1 << 1
	AENotWait        = 1 << 2
	AECallBeforeWait = 1 << 3
	AECallAfterWait  = 1 << 4
	AEAllEvent       = AETimeEv | AEIOEv
)

const ioEvEnqueueTimeout = 1 * time.Second

type BeforeWaitCB func()
type AfterWaitCB func()

type AeLoop struct {
	timeEvHead   *AeTimeEv
	lastTime     time.Time
	dsp          *Dispatcher
	beforeWait   BeforeWaitCB
	afterWait    AfterWaitCB
	apidata      interface{}
	nextTimeEvID int64
	flags        int32
	stop         int32
}

func NewAeLoop(beforeWait BeforeWaitCB, afterWait AfterWaitCB) *AeLoop {
	var loop *AeLoop
	loop = new(AeLoop)

	loop.stop = 0
	loop.flags = 0
	loop.lastTime = time.Now()
	loop.timeEvHead = nil
	loop.dsp = NewDispatcher(1<<10, 1<<10)
	return loop
}

func (ae *AeLoop) Stop() {
	ae.stop = 1
}

func (ae *AeLoop) IsStop() bool {
	return ae.stop == 1
}

func (ae *AeLoop) CreateTimeEv(after time.Duration, cb AeTimeEvCB, fcb AeTimeEvFiniCB, ctx context.Context, finit AeTimeEvFiniCB) {
	tev := newAeTimeEv(ae.nextTimeEvID, time.Now().Add(after), cb, fcb, ctx)
	ae.nextTimeEvID++
	tev.prev = nil
	tev.next = ae.timeEvHead
	if tev.next != nil {
		tev.next.prev = tev
	}
	ae.timeEvHead = tev
}

func (ae *AeLoop) CreateIOEv(ctx context.Context, requestFn AeRequest, cb AeIOCB) error {
	if ae.dsp.NewJobWithTimeout(requestFn, cb, ioEvEnqueueTimeout) == ErrTimeout {
		return ErrBusy
	}
	return nil
}

func (ae *AeLoop) DeleteTimeEv(id int64) error {
	var ev *AeTimeEv = ae.timeEvHead
	for ev != nil {
		if ev.id == id {
			ev.id = TimeEvDeleted
			return nil
		}
		ev = ev.next
	}
	return errors.Wrapf(ErrEventNotFound, "%d", id)
}

func (ae *AeLoop) aeSearchNearestTimer() *AeTimeEv {
	var ev, nearest *AeTimeEv
	ev = ae.timeEvHead
	for ev != nil {
		if nearest == nil || nearest.whenTime.After(ev.whenTime) {
			nearest = ev
		}
		ev = ev.next
	}
	return nearest
}

func (ae *AeLoop) processTimeEv() int {
	var (
		te        *AeTimeEv
		processed int
		maxId     int64
		now       time.Time
	)
	now = time.Now()
	if ae.lastTime.Sub(now) > 0 {
		te = ae.timeEvHead
		for te != nil {
			te.whenTime = time.Date(0, 0, 0, 0, 0, 0, 0, time.UTC)
			te = te.next
		}
	}
	ae.lastTime = now

	te = ae.timeEvHead
	maxId = ae.nextTimeEvID - 1
	for te != nil {
		// this time event is deleted
		if te.id == TimeEvDeleted {
			next := te.next

			if te.refcount != 0 {
				te = next
				continue
			}
			if te.prev != nil {
				te.prev.next = next
			} else {
				ae.timeEvHead = next
			}

			if te.next != nil {
				next.prev = te.prev
			}
			if te.fcb != nil {
				te.fcb(te.ctx)
			}
			te = next
			continue
		}

		// do not process event created by this iteration
		if te.id > maxId {
			te = te.next
			continue
		}
		now = time.Now()
		if now.After(te.whenTime) {
			te.refcount++
			againAfter := te.cb(te.ctx, te.id)
			te.refcount--
			processed++
			if againAfter > 0 {
				te.whenTime = te.whenTime.Add(againAfter)
			} else {
				te.id = TimeEvDeleted
			}
		}
		te = te.next
	}
	return processed
}

func (ae *AeLoop) AeProcessEvents(flags Flag) (int, error) {
	var (
		processed int
		shortest  *AeTimeEv
		interval  time.Duration
		ctx       context.Context
		cancel    context.CancelFunc
	)
	if (flags&AETimeEv) == 0 && (flags&AEIOEv) == 0 {
		return processed, nil
	}

	if ae.dsp.Doing() != 0 ||
		((flags&AETimeEv) == 1 && (flags&AENotWait) == 0) {

		if (flags&AETimeEv) == 1 && (flags&AENotWait) == 0 {
			shortest = ae.aeSearchNearestTimer()
		}
		if shortest != nil {
			now := time.Now()
			interval = shortest.whenTime.Sub(now)
		} else {
			if (flags & AENotWait) == 1 {
				interval = 0
			} else {
				interval = -1
			}
		}

		if (ae.flags & AENotWait) == 1 {
			interval = -1
		}

		if ae.beforeWait != nil && (flags&AECallBeforeWait) == 1 {
			ae.beforeWait()
		}

		if interval != -1 {
			ctx, cancel = context.WithTimeout(context.Background(), interval)
			defer cancel()
		} else {
			ctx = context.Background()
		}
		events, err := ae.dsp.GetDoneJobs(ctx)
		if err != nil {
			return processed, err
		}

		if ae.afterWait != nil && (flags&AECallAfterWait) == 1 {
			ae.afterWait()
		}

		for _, ev := range events {
			ev.Callback(ev.ClientData)
			processed++
		}
	}
	if (flags & AETimeEv) == 1 {
		processed += ae.processTimeEv()
	}
	return processed, nil
}
