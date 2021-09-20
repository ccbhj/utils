package utils

import (
	"context"
	"log"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"
)

var ErrClosed error = errors.New("dispatcher closed")
var ErrTimeout error = errors.New("enqueue timeout")

const maxResume = 3

type Job struct {
	fn func()
	ID string
}

type jobCh chan Job

type worker struct {
	workerPool chan jobCh
	ch         jobCh
	quit       chan struct{}
	id         int
	nresume    int
	wg         *sync.WaitGroup
}

type Dispatcher struct {
	workerPool chan jobCh
	jobQueue   chan Job
	workers    []*worker
	quit       chan struct{}
	wg         *sync.WaitGroup
	runOnce    sync.Once

	closed int32
	doing  int64
}

func newWorker(id int, pool chan jobCh, wg *sync.WaitGroup) *worker {
	wk := &worker{
		workerPool: pool,
		ch:         make(chan Job),
		quit:       make(chan struct{}, 1),
		id:         id,
		wg:         wg,
	}
	return wk
}

func goSafe(fn func(), fini func(), onPanic func(interface{}), rec bool) {
	if fn == nil {
		panic("can't goSafe nil func")
	}
	go func() {
		defer func() {
			if !rec {
				return
			}
			if r := recover(); r != nil {
				if onPanic != nil {
					onPanic(r)
				}
				if fini != nil {
					fini()
				}
			}
		}()
		fn()
		if fini != nil {
			fini()
		}
	}()
}

func (w *worker) work() {
	for {
		// send the w.ch to workPool to notify the dispather to schedule jobs
		w.workerPool <- w.ch
		// wait for a job or quit
		select {
		case j := <-w.ch:
			j.fn()
		case <-w.quit:
			return
		}
	}
}

func (w *worker) startWork() {
	goSafe(w.work,
		func() {
			w.wg.Done()
			log.Printf("worker[id=%d] exit", w.id)
		},
		func(r interface{}) {
			log.Printf("panic_on_worker[id=%d]|err=%s", w.id, r)
			w.nresume++
			if w.nresume > maxResume {
				log.Printf("worker_max_resume[id=%d]", w.id)
				return
			}
			w.startWork()
		}, true)
}

func NewDispatcher(queueSize, nworker int) *Dispatcher {
	d := new(Dispatcher)
	d.workerPool = make(chan jobCh, nworker)
	d.jobQueue = make(chan Job, queueSize)
	d.workers = make([]*worker, nworker)
	d.quit = make(chan struct{}, 1)
	d.wg = &sync.WaitGroup{}
	d.closed = 0

	d.wg.Add(nworker)
	for i := 0; i < nworker; i++ {
		d.workers[i] = newWorker(i, d.workerPool, d.wg)
		d.workers[i].startWork()
	}
	return d
}

func (d *Dispatcher) isStop() bool {
	return atomic.LoadInt32(&d.closed) == 1
}

func (d *Dispatcher) dispatch() {
loop:
	for !d.isStop() {
		select {
		case <-d.quit:
			break loop
		case job := <-d.jobQueue:
			// wait for a worker to be ready
			ch := <-d.workerPool
			// wrap the job
			fn := job.fn
			job.fn = func() {
				defer func() {
					if r := recover(); r != nil {
						d.afterPanic(job, r)
					}
				}()
				d.beforeJob(job)
				fn()
				d.afterJob(job)
			}
			ch <- job
		}
	}
	close(d.jobQueue)
}

// hooks
func (d *Dispatcher) beforeJob(job Job) {
	log.Printf("start job[%s]", job.ID)
	atomic.AddInt64(&d.doing, 1)
}

func (d *Dispatcher) afterJob(job Job) {
	log.Printf("exit job[%s]", job.ID)
	atomic.AddInt64(&d.doing, -1)
}

func (d *Dispatcher) afterPanic(job Job, msg interface{}) {
	log.Printf("job[%s] panic|msg=%s", job.ID, msg)
}

func (d *Dispatcher) Run() {
	d.runOnce.Do(func() {
		go d.dispatch()
	})
}

func (d *Dispatcher) Stop() []Job {
	doneJobs := make([]Job, 0)
	atomic.StoreInt32(&d.closed, 1)
	d.quit <- struct{}{}
	for _, wk := range d.workers {
		wk.quit <- struct{}{}
	}
	d.wg.Wait()
	return doneJobs
}

func (d *Dispatcher) Doing() int64 {
	return atomic.LoadInt64(&d.doing)
}

func (d *Dispatcher) NewJob(id string, fn func()) error {
	if d.isStop() {
		return ErrClosed
	}
	j := Job{
		fn: fn,
		ID: id,
	}
	d.jobQueue <- j
	return nil
}

func (d *Dispatcher) NewJobWithTimeout(ctx context.Context, fn func(), id string) error {
	if d.isStop() {
		return ErrClosed
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case d.jobQueue <- Job{fn: fn, ID: id}:
		return nil
	}
}
