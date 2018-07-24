package majordomo_test

import (
	"fmt"
	"log"
	"testing"
	"time"

	md "github.com/iamdobi/majordomo-go"
	"github.com/iamdobi/majordomo-go/mdapi"
)

func TestPurge(t *testing.T) {
	port := 5550
	verbose := false
	broker := startBroker(port, false, md.HeartbeatInterval(200*time.Millisecond))

	workerNum := 3
	var workers []*mdapi.Mdwrk
	for i := 0; i < workerNum; i++ {
		workers = append(workers, startWorker(port, verbose, mdapi.HeartbeatInterval(200*time.Millisecond)))
	}
	time.Sleep(100 * time.Millisecond)
	delWorker := workers[len(workers)-1]
	workers = workers[0 : len(workers)-1]
	delWorker.Close()

	time.Sleep(800 * time.Millisecond)

	// broker should have only two workers
	workerLen := broker.WorkerLen()
	if workerLen != workerNum-1 {
		t.Errorf("broker should have threw one worker! [%d]", workerLen)
	}

	for _, worker := range workers {
		worker.Close()
	}
	broker.Close()
}

func TestReqRep(t *testing.T) {
	port := 5551
	verbose := false
	broker := startBroker(port, verbose)
	worker := startWorker(port, verbose)

	time.Sleep(300 * time.Millisecond)

	cliapi, err := mdapi.NewMdcli(fmt.Sprintf("tcp://localhost:%d", port), verbose)
	reply, err := cliapi.Send("echo", []byte("hello"))
	if err != nil {
		t.Error(err)
	}

	replystr := string(reply[0])
	if replystr != "hello" {
		t.Errorf("reply should hello! [%s]", replystr)
	}

	broker.Close()
	worker.Close()

	time.Sleep(500 * time.Millisecond) // time to release
}

func startBroker(port int, verbose bool, setters ...md.Option) *md.Broker {
	broker := md.StartBroker(port, verbose, setters...)
	return broker
}

func startWorker(port int, verbose bool, setters ...mdapi.Option) *mdapi.Mdwrk {
	session, _ := mdapi.NewMdwrk(fmt.Sprintf("tcp://localhost:%d", port), "echo", verbose, setters...)

	go func(session *mdapi.Mdwrk) {
		var err error
		var request, reply [][]byte
		for {
			request, err = session.Recv(reply)
			if err != nil {
				fmt.Printf("worker interrupted..\n")
				break //  Worker was interrupted
			}
			reply = request //  Echo is complex... :-)
			log.Printf("[worker] reply=%v\n", reply)
		}
	}(session)

	return session
}
