// Majordomo Protocol Worker API.
// Implements the MDP/Worker spec at http://rfc.zeromq.org/spec:7.

package mdapi

import (
	zmq "github.com/pebbe/zmq4"

	"log"
	"runtime"
	"time"
)

const (
	heartbeat_liveness = 3 //  3-5 is reasonable
)

// Options worker options
type Options struct {
	heartbeatLiveness int
	heartbeatInterval time.Duration
	reconnectInterval time.Duration
}

type Option func(*Options)

func HeartbeatLiveness(heartbeatLiveness int) Option {
	return func(args *Options) {
		args.heartbeatLiveness = heartbeatLiveness
	}
}

func HeartbeatInterval(heartbeatInterval time.Duration) Option {
	return func(args *Options) {
		args.heartbeatInterval = heartbeatInterval
	}
}

func ReconnectInterval(reconnectInterval time.Duration) Option {
	return func(args *Options) {
		args.reconnectInterval = reconnectInterval
	}
}

//  This is the structure of a worker API instance. We use a pseudo-OO
//  approach in a lot of the C examples, as well as the CZMQ binding:

//  Structure of our class
//  We access these properties only via class methods

// Mdwrk Majordomo Protocol Worker API.
type Mdwrk struct {
	broker  string
	service string
	worker  *zmq.Socket //  Socket to broker
	poller  *zmq.Poller
	options *Options
	verbose bool //  Print activity to stdout

	//  Heartbeat management
	heartbeat_at time.Time //  When to send HEARTBEAT
	liveness     int       //  How many attempts left

	expect_reply bool   //  False only at start
	reply_to     string //  Return identity, if any
}

//  We have two utility functions; to send a message to the broker and
//  to (re-)connect to the broker:

//  ---------------------------------------------------------------------

//  SendToBroker Send message to broker.
func (mdwrk *Mdwrk) SendToBroker(command string, option string, msg [][]byte) (err error) {

	n := 3
	if option != "" {
		n++
	}
	m := make([][]byte, n, n+len(msg))
	m = append(m, msg...)

	//  Stack protocol envelope to start of message
	if option != "" {
		m[3] = []byte(option)
	}
	m[2] = []byte(command)
	m[1] = []byte(MDPW_WORKER)
	m[0] = []byte("")

	if mdwrk.verbose {
		log.Printf("I: sending %s to broker %q\n", MDPS_COMMANDS[command], m)
	}
	_, err = mdwrk.worker.SendMessage(m)
	return
}

//  ---------------------------------------------------------------------

//  ConnectToBroker Connect or reconnect to broker.
func (mdwrk *Mdwrk) ConnectToBroker() (err error) {
	if mdwrk.worker != nil {
		mdwrk.worker.Close()
		mdwrk.worker = nil
	}
	mdwrk.worker, err = zmq.NewSocket(zmq.DEALER)
	err = mdwrk.worker.Connect(mdwrk.broker)
	if mdwrk.verbose {
		log.Printf("I: connecting to broker at %s...\n", mdwrk.broker)
	}
	mdwrk.poller = zmq.NewPoller()
	mdwrk.poller.Add(mdwrk.worker, zmq.POLLIN)

	//  Register service with broker
	err = mdwrk.SendToBroker(MDPW_READY, mdwrk.service, [][]byte{})

	//  If liveness hits zero, queue is considered disconnected
	mdwrk.liveness = mdwrk.options.heartbeatLiveness
	mdwrk.heartbeat_at = time.Now().Add(mdwrk.options.heartbeatInterval)

	return
}

//  Here we have the constructor and destructor for our mdwrk class:

//  ---------------------------------------------------------------------
//  NewMdwrk Constructor
func NewMdwrk(broker, service string, verbose bool, setters ...Option) (mdwrk *Mdwrk, err error) {
	// Default Options
	args := &Options{
		heartbeatLiveness: 3,
		heartbeatInterval: 2500 * time.Millisecond,
		reconnectInterval: 2500 * time.Millisecond,
	}

	for _, setter := range setters {
		setter(args)
	}

	mdwrk = &Mdwrk{
		broker:  broker,
		service: service,
		verbose: verbose,
		options: args,
	}

	err = mdwrk.ConnectToBroker()

	runtime.SetFinalizer(mdwrk, (*Mdwrk).Close)

	return
}

//  ---------------------------------------------------------------------
//  Close Destructor
func (mdwrk *Mdwrk) Close() {
	if mdwrk.worker != nil {
		mdwrk.worker.SetLinger(0)
		mdwrk.worker.Close()
		mdwrk.worker = nil
	}
}

//  This is the recv method; it's a little misnamed since it first sends
//  any reply and then waits for a new request. If you have a better name
//  for this, let me know:

//  ---------------------------------------------------------------------

//  Recv Send reply, if any, to broker and wait for next request.
func (mdwrk *Mdwrk) Recv(reply [][]byte) (msg [][]byte, err error) {
	//  Format and send the reply if we were provided one
	if len(reply) == 0 && mdwrk.expect_reply {
		panic("No reply, expected")
	}
	if len(reply) > 0 {
		if mdwrk.reply_to == "" {
			panic("mdwrk.reply_to == \"\"")
		}
		m := make([][]byte, 2, 2+len(reply))
		m = append(m, reply...)
		m[0] = []byte(mdwrk.reply_to)
		m[1] = []byte("")
		err = mdwrk.SendToBroker(MDPW_REPLY, "", m)
	}
	mdwrk.expect_reply = true

	for {
		var polled []zmq.Polled
		polled, err = mdwrk.poller.Poll(mdwrk.options.heartbeatInterval)
		if err != nil {
			break //  Interrupted
		}

		if len(polled) > 0 {
			msg, err = mdwrk.worker.RecvMessageBytes(0)
			if err != nil {
				break //  Interrupted
			}
			if mdwrk.verbose {
				log.Printf("I: received message from broker: %q\n", msg)
			}
			mdwrk.liveness = mdwrk.options.heartbeatLiveness

			//  Don't try to handle errors, just assert noisily
			if len(msg) < 3 {
				panic("len(msg) < 3")
			}

			if len(msg[0]) > 0 {
				panic("msg[0] != \"\"")
			}

			if string(msg[1]) != MDPW_WORKER {
				panic("msg[1] != MDPW_WORKER")
			}

			command := string(msg[2])
			msg = msg[3:]
			switch command {
			case MDPW_REQUEST:
				mdwrk.sendAck(msg[0])
				msg = msg[1:]

				//  We should pop and save as many addresses as there are
				//  up to a null part, but for now, just save one...
				var replyTob []byte
				replyTob, msg = unwrap(msg)
				mdwrk.reply_to = string(replyTob)
				//  Here is where we actually have a message to process; we
				//  return it to the caller application:
				return //  We have a request to process
			case MDPW_HEARTBEAT:
				//  Do nothing for heartbeats
			case MDPW_DISCONNECT:
				mdwrk.ConnectToBroker()
			default:
				log.Printf("E: invalid input message %q\n", msg)
			}
		} else {
			mdwrk.liveness--
			if mdwrk.liveness == 0 {
				if mdwrk.verbose {
					log.Println("W: disconnected from broker - retrying...")
				}
				time.Sleep(mdwrk.options.reconnectInterval)
				mdwrk.ConnectToBroker()
			}
		}
		//  Send HEARTBEAT if it's time
		if time.Now().After(mdwrk.heartbeat_at) {
			mdwrk.SendToBroker(MDPW_HEARTBEAT, "", [][]byte{})
			mdwrk.heartbeat_at = time.Now().Add(mdwrk.options.heartbeatInterval)
		}
	}
	return
}

func (mdwrk *Mdwrk) sendAck(pktid []byte) error {
	if mdwrk.verbose {
		log.Printf("W: worker send ack %s\n", pktid)
	}
	m := [][]byte{pktid}
	return mdwrk.SendToBroker(MDPW_ACK, "", m)
}

//  Pops frame off front of message and returns it as 'head'
//  If next frame is empty, pops that empty frame.
//  Return remaining frames of message as 'tail'
func unwrap(msg [][]byte) (head []byte, tail [][]byte) {
	head = msg[0]
	if len(msg) > 1 && len(msg[1]) == 0 {
		tail = msg[2:]
	} else {
		tail = msg[1:]
	}
	return
}
