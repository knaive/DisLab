package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"

type ViewServer struct {
	mu   sync.Mutex
	l    net.Listener
	dead bool
	me   string

	// Your declarations here.
	views        [4]View
	cur          int
	latest       int
	primaryTimer int
	backupTimer  int
    ackedView    uint
	lock         chan string
}

func (vs *ViewServer) nextView() View {
	index := vs.cur + 1
	if index > vs.latest {
		index = vs.latest
	}
	return vs.views[index%4]
}
func (vs *ViewServer) setView(view View) {
    fmt.Println(view, vs.currentView())
	vs.latest++
	vs.views[vs.latest%4] = view
}
func (vs *ViewServer) currentView() View {
	return vs.views[vs.cur%4]
}
func (vs *ViewServer) updateView(ack uint) {
    vs.ackedView = ack
    if vs.cur < vs.latest {
        vs.cur++
    }
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

	// Your code here.
	vs.mu.Lock()
	vs.lock <- "locked"
	view := vs.currentView()
	if view.Primary == "" {
		if view.Backup == "" {
			if args.Viewnum == view.Viewnum {
				vs.setView(View{Viewnum: 1, Primary: args.Me, Backup: ""})
                vs.updateView(0)
			}
		} 
	} else {
		if args.Me == view.Primary {
			if args.Viewnum >= view.Viewnum {
				vs.updateView(args.Viewnum)
			} 
            if args.Viewnum == 0 {
                if vs.ackedView == view.Viewnum {
                    vs.setView(View{Viewnum:view.Viewnum+1, Primary:view.Backup, Backup:""})
                    vs.updateView(vs.ackedView)
                }
            }
		} else if view.Backup == "" {
			v := view
			v.Backup = args.Me
			v.Viewnum = vs.nextView().Viewnum+1
			vs.setView(v)
            if vs.ackedView == view.Viewnum {
                vs.updateView(vs.ackedView)
            }
		}
	}

	if args.Me == view.Primary {
		vs.primaryTimer = 0
	} else if args.Me == view.Backup {
		vs.backupTimer = 0
	}
	reply.View = vs.currentView()
	<-vs.lock
	vs.mu.Unlock()

	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	reply.View = vs.currentView()

	return nil
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {

	// Your code here.
	vs.lock <- "locked"
	vs.primaryTimer++
	view := vs.currentView()
	if vs.primaryTimer == 5 {
        if vs.ackedView == view.Viewnum {
            num := uint(vs.latest + 1)
            vs.setView(View{Primary: view.Backup, Backup: "", Viewnum: num})
            vs.updateView(vs.ackedView)
        }
        vs.primaryTimer = 0
	}

	vs.backupTimer++
	if vs.backupTimer == 5 {
        if vs.ackedView == view.Viewnum {
            num := uint(vs.latest + 1)
            vs.setView(View{Primary: view.Primary, Backup: "", Viewnum: num})
            vs.updateView(vs.ackedView)
        }
		vs.backupTimer = 0
	}
	<-vs.lock
}

//
// tell the server to shut itself down.
// for testing.
// please don't change this function.
//
func (vs *ViewServer) Kill() {
	vs.dead = true
	vs.l.Close()
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	vs.mu = sync.Mutex{}
	vs.dead = false
	// vs.view = View{Viewnum: 0, Primary: "", Backup: ""}
	// vs.newView = View{Viewnum: 0, Primary: "", Backup: ""}
	vs.primaryTimer = 0
	vs.backupTimer = 0
	vs.lock = make(chan string, 1)
	vs.cur = 0
	vs.latest = 0
    vs.ackedView = 0
	// Your vs.* initializations here.

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.dead == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.dead == false {
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.dead == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.dead == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
