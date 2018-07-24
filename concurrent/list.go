package concurrent

// List ...
type List interface {
	GetAt(idx int) interface{}
	Add(elem interface{})
	RemoveAt(idx int) interface{}
	Len() int
	Iterator(func(elem interface{}))
}

// ConcurrentList ..
type ConcurrentList struct {
	backingStore []interface{}
	requestChan  chan RequestArgs
	responseChan chan interface{}
}

const (
	REQ_GET    = 1
	REQ_ADD    = 2
	REQ_REMOVE = 3
	REQ_LEN    = 4
)

type RequestArgs struct {
	requestCode int
	args        []interface{}
}

func NewConcurrentList() List {
	clist := &ConcurrentList{
		requestChan:  make(chan RequestArgs),
		responseChan: make(chan interface{}),
	}

	go func() {
		for {
			select {
			case req := <-clist.requestChan:
				switch req.requestCode {
				case REQ_GET:
					clist.responseChan <- clist.backingStore[(req.args[0]).(int)]
				case REQ_ADD:
					clist.backingStore = append(clist.backingStore, req.args[0])
				case REQ_REMOVE:
					i := (req.args[0]).(int)
					retVal := clist.backingStore[i]

					copy(clist.backingStore[i:], clist.backingStore[i+1:])
					clist.backingStore[len(clist.backingStore)-1] = nil
					clist.backingStore = clist.backingStore[:len(clist.backingStore)-1]

					clist.responseChan <- retVal
				case REQ_LEN:
					clist.responseChan <- len(clist.backingStore)
				}
			}
		}
	}()

	return clist
}

func (list *ConcurrentList) GetAt(idx int) interface{} {
	list.requestChan <- RequestArgs{
		requestCode: REQ_GET,
		args:        []interface{}{idx},
	}
	return <-list.responseChan
}

func (list *ConcurrentList) Add(elem interface{}) {
	list.requestChan <- RequestArgs{
		requestCode: REQ_ADD,
		args:        []interface{}{elem},
	}
}

func (list *ConcurrentList) RemoveAt(idx int) interface{} {
	list.requestChan <- RequestArgs{
		requestCode: REQ_REMOVE,
		args:        []interface{}{idx},
	}
	return <-list.responseChan
}

func (list *ConcurrentList) Len() int {
	list.requestChan <- RequestArgs{
		requestCode: REQ_LEN,
		args:        []interface{}{},
	}
	return (<-list.responseChan).(int)
}

func (list *ConcurrentList) Iterator(iterFunc func(elem interface{})) {
	copied := list.backingStore[0:]
	for _, e := range copied {
		iterFunc(e)
	}
}

func (list *ConcurrentList) Close() {
	close(list.requestChan)
	close(list.responseChan)
}
