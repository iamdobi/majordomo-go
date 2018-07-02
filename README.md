# majordomo-go
Majordomo Project for Go

Majordomo pattern: http://zguide.zeromq.org/page:all#Service-Oriented-Reliable-Queuing-Majordomo-Pattern

This project is copy of **"https://github.com/pebbe/zmq4/blob/master/examples/mdbroker.go"**, only to support []byte type messages.

## Examples

### Start Broker
```go
package main

import (
  "fmt"
  md "github.com/iamdobi/majordomo-go"
)

func main() {
  fmt.Println("vim-go")

  md.StartBroker(5555, true)
}

```

### Start Worker
```go
package main

import (
  "github.com/iamdobi/majordomo-go/mdapi"
  "log"
)

func main() {
  verbose := true
  session, _ := mdapi.NewMdwrk("tcp://localhost:5555", "echo", verbose)

  var err error
  var request, reply [][]byte
  for {
    request, err = session.Recv(reply)
    if err != nil {
      break //  Worker was interrupted
    }
    reply = request //  Echo is complex... :-)
    log.Printf("[worker] reply=%v\n", reply)
  }
  log.Println(err)
}
```

### Start Client
```go
package main

import (
  "github.com/iamdobi/majordomo-go/mdapi"
  "log"
)

func main() {
  verbose := true
  session, _ := mdapi.NewMdcli("tcp://localhost:5555", verbose)

  count := 0
  for ; count < 10; count++ {
    reply, err := session.Send("echo", []byte("Hello world"))
    if err != nil {
      log.Println(err)
      break //  Interrupt or failure
    } else {
      log.Printf("[client] reply=%v\n", string(reply[0]))
    }
  }
  log.Printf("%d requests/replies processed\n", count)
}
```
