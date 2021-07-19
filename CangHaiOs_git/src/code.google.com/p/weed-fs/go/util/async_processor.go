package util

import (
	"container/list"
	"fmt"
	_ "fmt"
	"sync"
	"time"
)

type AsyncRequest struct {
	requestId string
	request   interface{}
}
type AsyncResponse struct {
	response interface{}
}
type AsyncTask struct {
	request    *AsyncRequest
	response   *AsyncResponse
	listHead   *list.Element
	status     int
	waitcount  int
	hit        int
	expireTime time.Time
	mlock      sync.Mutex
	cond       *sync.Cond
}

type TaskStatus struct {
	Reqid      string    `json:"Reqid"`
	Status     int       `json:"Status"`
	Waitcount  int       `json:"Waitcount"`
	Hit        int       `json:"Hit"`
	ExpireTime time.Time `json:"ExpireTime"`
}

const (
	ACCEPTED   = iota
	INQUEUE
	PROCESSING
	DONE
)

type AsyncTaskGroup struct {
	name       string
	tasks      map[string]*AsyncTask
	taskCh     chan *AsyncTask
	lru        *list.List
	goCount    int
	goMaxCount int
	cacheTime  int
	reqTotal   int64
	hit        int64
	unhit      int64
	lock       sync.RWMutex
	chlock     sync.Mutex
}

func (atg *AsyncTaskGroup) DoTask(reqId string, req interface{}) (resp interface{}) {

	atg.lock.Lock()
	task, ok := atg.tasks[reqId]
	if !ok {
		atg.unhit++
		task = &AsyncTask{}
		task.cond = sync.NewCond(&task.mlock)
		task.request = &AsyncRequest{requestId: reqId, request: req}
		task.expireTime = time.Now().Add(time.Duration(1) * time.Hour)
		task.status = ACCEPTED
		task.hit++
		task.waitcount = 1
		atg.tasks[reqId] = task
		atg.lru.PushBack(task)
		task.listHead = atg.lru.Back()
		atg.lock.Unlock()
		task.status = INQUEUE
		atg.taskCh <- task
	} else {
		atg.hit++
		task.waitcount++
		task.hit++
		atg.lock.Unlock()
	}
	task.mlock.Lock()
	if task.status != DONE {

		for task.status != DONE {
			task.cond.Wait()
		}

	}
	task.mlock.Unlock()
	atg.lock.Lock()
	if task, ok := atg.tasks[reqId]; ok {
		task.mlock.Lock()
		task.waitcount--
		if task.status == DONE && task.waitcount == 0 {
			if atg.cacheTime == 0 {
				delete(atg.tasks, reqId)
				atg.lru.Remove(task.listHead)
			} else {
				atg.lru.MoveToBack(task.listHead)
				task.expireTime = time.Now().Add(time.Duration(atg.cacheTime) * time.Second)
			}
		}
		task.mlock.Unlock()
	}
	atg.reqTotal++
	atg.lock.Unlock()
	return task.response.response
}
func NewAsyncTaskGroup(name string, doReq func(interface{}) interface{}, taskQueLen, taskConnum, cacheTime int) *AsyncTaskGroup {
	atg := &AsyncTaskGroup{name: name, goMaxCount: taskConnum, cacheTime: cacheTime}
	atg.tasks = make(map[string]*AsyncTask, taskQueLen)
	atg.taskCh = make(chan *AsyncTask, taskQueLen)
	atg.goCount = 0
	atg.lru = list.New()
	go atg.asyncProcessor(doReq)
	if cacheTime > 0 {
		go atg.cacheCleanWorker()
	}
	/*
		go func() {
			for {
				fmt.Println("name:", atg.name, "totCount:", atg.reqTotal, "hit:", atg.hit, "unhit:", atg.unhit)
				time.Sleep(1 * time.Second)
			}

		}()
	*/

	return atg
}
func (atg *AsyncTaskGroup) asyncProcessor(doReq func(req interface{}) interface{}) {
	atg.chlock.Lock()
	atg.goCount++
	atg.chlock.Unlock()
	for {
		task := <-atg.taskCh
		go atg.asyncProcessor(doReq)

		resp := doReq(task.request.request)
		task.mlock.Lock()
		task.response = &AsyncResponse{response: resp}
		task.status = DONE
		task.expireTime = time.Now().Add(time.Duration(atg.cacheTime) * time.Second)
		task.cond.Broadcast()
		task.mlock.Unlock()
		atg.chlock.Lock()
		if atg.goCount > atg.goMaxCount {
			atg.goCount--
			break
		}
		atg.chlock.Unlock()
	}
	atg.chlock.Unlock()

}

const BATCH_CLEAN_MAX_NUM = 100

func (atg *AsyncTaskGroup) cacheCleanWorker() {
	for {
		atg.lock.Lock()
		i := 0
		e := atg.lru.Front()
		for e != nil {
			ele := e.Next()
			if asyncTask, ok := e.Value.(*AsyncTask); ok {
				asyncTask.mlock.Lock()
				if asyncTask.expireTime.Before(time.Now()) && asyncTask.waitcount <= 0 {
					//fmt.Println("delete request:", asyncTask.request.requestId)
					delete(atg.tasks, asyncTask.request.requestId)
					atg.lru.Remove(asyncTask.listHead)

				}
				asyncTask.mlock.Unlock()
				i++
				if i >= BATCH_CLEAN_MAX_NUM {
					break
				}

			}
			e = ele

		}
		atg.lock.Unlock()
		if i < BATCH_CLEAN_MAX_NUM {
			time.Sleep(100 * time.Millisecond)
		}
	}

}
func (atg *AsyncTaskGroup) GetDebugInfo() string {
	str := ""
	atg.lock.RLock()
	str += fmt.Sprintf("name=%s,tasks=%d,lru=%d,hit=%d,unhit=%d", atg.name, len(atg.tasks), atg.lru.Len(), atg.hit, atg.unhit)

	atg.lock.RUnlock()
	return str
}

func (atg *AsyncTaskGroup) CleanHit() {
	atg.lock.Lock()
	defer atg.lock.Unlock()
	atg.hit = 0
	atg.unhit = 0
}

//查看缓存中是不是存在，
func (atg *AsyncTaskGroup) GetFileHit(reqId string) []*TaskStatus {
	atg.lock.Lock()
	defer atg.lock.Unlock()
	if reqId == "" {
		fmt.Println(len(atg.tasks))
		res := []*TaskStatus{}
		count := 50
		for _, v := range atg.tasks {
			sta := &TaskStatus{
				Reqid:      v.request.requestId,
				Status:     v.status,
				Waitcount:  v.waitcount,
				Hit:        v.hit,
				ExpireTime: v.expireTime,
			}
			res = append(res, sta)
			count--
			if count == 0 {
				break
			}
		}
		return res
	} else {
		if task, ok := atg.tasks[reqId]; ok {
			sta := &TaskStatus{
				Reqid:      task.request.requestId,
				Status:     task.status,
				Waitcount:  task.waitcount,
				Hit:        task.hit,
				ExpireTime: task.expireTime,
			}
			return []*TaskStatus{sta}
		} else {
			return nil
		}
	}
}
