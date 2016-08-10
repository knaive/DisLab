package mapreduce
import "container/list"
import "fmt"

type WorkerInfo struct {
  address string
  free bool
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

func (mr *MapReduce) FindFree(workerReady chan string) string {
    for {
        for addr, info := range mr.Workers {
            if info.free {
                return addr
            }
        }
        <-workerReady
    }
}

func (mr *MapReduce) Schedule(operation JobType, round int, failedWorkers, freeWorkers, workerReady chan string, done chan bool) {
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
        DPrintf("%s, %s, round %d\n", addr, operation, round)
        if ok {
            if _, exist := mr.Workers[addr]; !exist {
                DPrintf("%s not exist", addr)
            }
            mr.Workers[addr].free = true
            done <- true
            workerReady <- addr
            return
        }
        DPrintf("%s failed,%s,round%d\n", addr, operation, round)
        failedWorkers <- addr
    }
}

func (mr *MapReduce) RunMaster() *list.List {
    // Your code here
    onePhaseDone := make(chan bool)
    failedWorkers := make(chan string)
    freeWorkers := make(chan string)
    workerReady := make(chan string)

    // purge failed workers
    go func() {
        for {
            addr := <-failedWorkers
            delete(mr.Workers, addr)
        }
    }()

    // find a free worker and assign a job to it
    go func() {
        for {
            addr := mr.FindFree(workerReady)
            mr.Workers[addr].free = false
            freeWorkers <- addr
        }
    }()
    
    // get new workers ready
    go func() {
        for {
            worker := <-mr.registerChannel
            info := &WorkerInfo{}
            info.address = worker
            info.free = true
            mr.Workers[worker] = info
            workerReady <- worker
        }
    }()

    for i := 0; i < mr.nMap; i++ {
        go mr.Schedule(Map, i, failedWorkers, freeWorkers, workerReady, onePhaseDone)
    }

    // map done
    for i := 0; i < mr.nMap; i++ {
        <-onePhaseDone
    }

    for i :=0; i < mr.nReduce; i++ {
        go mr.Schedule(Reduce, i, failedWorkers, freeWorkers, workerReady, onePhaseDone)
    }

    // reduce done
    for i := 0; i < mr.nReduce; i++ {
        <-onePhaseDone
    }

    return mr.KillWorkers()
}
