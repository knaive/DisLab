package mapreduce
import "container/list"
import "fmt"

type WorkerInfo struct {
  address string
}

// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
  l := list.New()
  for _, w := range mr.Workers {
    DPrintf("DoWork: shutdown %s\n", w.address)
    args := &ShutdownArgs{}
    var reply ShutdownReply;
    ok := call(w.address, "Worker.Shutdown", args, &reply)
    if ok == false {
      fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
    } else {
      l.PushBack(reply.Njobs)
    }
  }
  return l
}

func (mr *MapReduce) Schedule(operation JobType, round int, freeWorkers chan string, done chan bool) {
    args := &DoJobArgs{}
    reply := &DoJobReply{}

    args.File = mr.file
    args.JobNumber = round
    args.Operation = operation
    if operation == Map {
        args.NumOtherPhase = mr.nReduce
    } else {
        args.NumOtherPhase = mr.nMap
    }

    for {
        addr := <-freeWorkers
        ok := call(addr, "Worker.DoJob", args, reply)
        if ok {
            done <- true
            freeWorkers <- addr
            return
        }
    }
}

func (mr *MapReduce) RunMaster() *list.List {
    // Your code here
    onePhaseDone := make(chan bool)
    freeWorkers := make(chan string)

    go func() {
        for {
            worker := <-mr.registerChannel
            freeWorkers <- worker
        }
    }()

    for i := 0; i < mr.nMap; i++ {
        go mr.Schedule(Map, i, freeWorkers, onePhaseDone)
    }

    // map done
    for i := 0; i < mr.nMap; i++ {
        <-onePhaseDone
    }

    for i :=0; i < mr.nReduce; i++ {
        go mr.Schedule(Reduce, i, freeWorkers, onePhaseDone)
    }

    // reduce done
    for i := 0; i < mr.nReduce; i++ {
        <-onePhaseDone
    }

    return mr.KillWorkers()
}
