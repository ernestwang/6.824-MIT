package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (decided bool, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"
import "os"
import "syscall"
import "sync"
import "fmt"
import "math/rand"
import "time"
import "io/ioutil"
import "container/list"

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       bool
	unreliable bool
	rpcCount   int
	peers      []string
	me         int // index into peers[]

	// Your data here.
	slots   map[int]Instance
	maxSlot int
	//mutex *sync.Mutex
}

var (
	LOGE = log.New(ioutil.Discard, "ERROR ", log.Lmicroseconds|log.Lshortfile)
	//LOGV = log.New(ioutil.Discard, "VERBOSE ", log.Lmicroseconds|log.Lshortfile)
	LOGV = log.New(os.Stdout, "VERBOSE ", log.Lmicroseconds|log.Lshortfile)
	LOGW = log.New(os.Stdout, "VERBOSE ", log.Lmicroseconds|log.Lshortfile)
)

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func chooseSeqNum(slotNum int, px *Paxos) int {
	return slotNum*len(px.peers) + px.me
}

func (px *Paxos) getInstance(seq int) (bool, Instance) {
	//px.mu.Lock()
	//defer px.mu.Unlock()
	instance, ok := px.slots[seq]
	if !ok {
		instance = Instance{-1, -1, false, nil}
		px.slots[seq] = instance
	}
	return instance.isDecided, instance
}

func (px *Paxos) propose(seq int, value interface{}) {
	LOGV.Printf("Propose: slot=%d\n", seq)
	majority := len(px.peers)/2 + 1
	agreed := false
	for slotNum := seq; !agreed; slotNum++ {
		seqNum := chooseSeqNum(slotNum, px)
		//var reply_prep [len(px.peers)]*PrepareReply
		//rpcReply := make(chan *PrepareReply, len(px.peers)-1)
		//closeReply := make(chan bool)
		/*
			go func() {
				<-time.After(50 * time.Millisecond)
				close(closeReply)
			}()
		*/
		replies := list.New()
		for i, peer := range px.peers {
			LOGV.Printf("prepare peer %d, %s\n", i, peer)

			//go func(peer string) {
			//LOGV.Printf("prepare peer %s\n", peer)
			args := &PrepareArgs{seqNum, slotNum}
			var reply PrepareReply
			ok := false
			if i == px.me {
				px.Prepare(args, &reply)
				ok = true
			} else {
				ok = call(peer, "Paxos.Prepare", args, &reply)

			}
			if ok {
				replies.PushBack(reply)
			}
			/*
				select {
				case <-closeReply:
					rpcReply <- new(PrepareReply)
				default:
					if ok {
						rpcReply <- &reply
					}
				}
			*/
			//}(peer)

		}
		maxAcceptedSeqNum := seqNum
		maxAcceptedValue := value
		prep_OK := 0
		for e := replies.Front(); e != nil; e = e.Next() {
			reply := e.Value.(PrepareReply)
			LOGV.Printf("reply to prepare: reply=%s\n", reply)
			if reply.OK {
				prep_OK++
				if reply.AcceptedSeqNum > maxAcceptedSeqNum {
					//LOGV.Printf("n_a=%d>n=%d, max_value=%s\n", reply.AcceptedSeqNum, maxAcceptedSeqNum, reply.AcceptedValue)

					maxAcceptedSeqNum = reply.AcceptedSeqNum
					maxAcceptedValue = reply.AcceptedValue
				}
			}
		}
		//LOGV.Printf("max_value=%s\n", maxAcceptedValue)
		/*
			for i := 0; i < len(px.peers)-1; i++ {
				LOGV.Printf("reply from peer %d\n", i)
				reply := <-rpcReply
				if reply.OK {
					prep_OK++
					if reply.AcceptedSeqNum > maxAcceptedSeqNum {
						//LOGV.Printf("n_a=%d>n=%d, max_value=%s\n", reply.AcceptedSeqNum, maxAcceptedSeqNum, reply.AcceptedValue)

						maxAcceptedSeqNum = reply.AcceptedSeqNum
						maxAcceptedValue = reply.AcceptedValue
					}
				}
			}
		*/
		LOGV.Printf("prepare : ok=%d\n", prep_OK)

		if prep_OK >= majority {
			LOGV.Printf("prepare reach quorum: ok=%d\n", prep_OK)
			rpcReply := make(chan *AcceptReply, len(px.peers)-1)
			closeReply := make(chan bool)

			go func() {
				<-time.After(50 * time.Millisecond)
				close(closeReply)
			}()

			for i, peer := range px.peers {
				args := &AcceptArgs{seqNum, slotNum, maxAcceptedValue}
				var reply AcceptReply
				if i == px.me {
					px.Accept(args, &reply)
				} else {
					go func(peer string) {
						ok := call(peer, "Paxos.Accept", args, &reply)
						select {
						case <-closeReply:
							rpcReply <- new(AcceptReply)
						default:
							if ok {
								rpcReply <- &reply
							}
						}
					}(peer)
				}

			} // end for accept
			accept_OK := 1
			for i := 0; i < len(px.peers)-1; i++ {
				reply := <-rpcReply
				if reply.OK {
					accept_OK++
				}
			}

			if accept_OK >= majority {
				LOGV.Printf("accept reach quorum: ok=%d\n", accept_OK)
				args := &DecideArgs{seqNum, slotNum, maxAcceptedValue}
				var reply DecideReply
				for i, peer := range px.peers {
					if i == px.me {
						px.Decide(args, &reply)
					} else {
						go func(peer string) {
							call(peer, "Paxos.Decide", args, &reply)
						}(peer)
					}
				}
				agreed = true
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	LOGV.Printf("Prepare: me=%d, slot=%d, seq=%d\n", px.me, args.SlotNum, args.SeqNum)
	_, instance := px.getInstance(args.SlotNum)
	if instance.preparedSeqNum < args.SeqNum {
		instance.preparedSeqNum = args.SeqNum
		px.slots[args.SlotNum] = instance
		reply.AcceptedSeqNum = instance.acceptedSeqNum
		reply.AcceptedValue = instance.value
		reply.OK = true
	} else {
		reply.OK = false
	}
	LOGV.Printf("Prepare return: me=%d, slot=%d, seq=%d\n", px.me, args.SlotNum, args.SeqNum)

	return nil
}

func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	//LOGV.Printf("Accept: me=%d, slot=%d, seq=%d\n", px.me, args.SlotNum, args.SeqNum)

	_, instance := px.getInstance(args.SlotNum)
	if instance.preparedSeqNum <= args.SeqNum {
		instance.preparedSeqNum = args.SeqNum
		instance.acceptedSeqNum = args.SeqNum
		instance.value = args.AcceptValue
		px.slots[args.SlotNum] = instance
		reply.AcceptedSeqNum = args.SeqNum
		reply.OK = true
	} else {
		reply.OK = false
	}
	return nil
}

func (px *Paxos) Decide(args *DecideArgs, reply *DecideReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	//LOGV.Printf("Decide: me=%d, slot=%d, seq=%d\n", px.me, args.SlotNum, args.SeqNum)

	decided, instance := px.getInstance(args.SlotNum)
	if !decided && instance.preparedSeqNum <= args.SeqNum {
		instance.preparedSeqNum = args.SeqNum
		instance.acceptedSeqNum = args.SeqNum
		instance.value = args.DecidedValue
		instance.isDecided = true
		px.slots[args.SlotNum] = instance

	}
	return nil
}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
	// Your code here.
	//px.mu.Lock()
	//defer px.mu.Unlock()
	LOGV.Printf("Start Paxos: peers=%d, me=%d, slot=%d\n", len(px.peers), px.me, seq)
	decided, _ := px.getInstance(seq)
	if !decided {
		px.propose(seq, v)
	}
	//LOGV.Printf("Start Paxos returned: peers=%d, me=%d, slot=%d\n", len(px.peers), px.me, seq)

}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	// Your code here.
	/*
		px.mu.Lock()
		defer px.mu.Unlock()

		//update my done map
		me := px.peers[px.me]
		px.maxPeerDones[me] = int(math.Max(float64(seq), float64(px.maxPeerDones[me])))
	*/
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	// Your code here.
	/*
		maxSeq := -1
		for inst := range px.instances {
			maxSeq = int(math.Max(float64(maxSeq), float64(inst)))
		}
		return maxSeq
	*/
	return 0
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
//
func (px *Paxos) Min() int {
	// You code here.
	/*
		me := px.peers[px.me]
		minSeq := px.maxPeerDones[me]
		for _, peerMin := range px.maxPeerDones {
			minSeq = int(math.Min(float64(minSeq), float64(peerMin)))
		}
		for instance := range px.instances {
			if minSeq >= instance {
				delete(px.instances, instance)
			}
		}
		return minSeq + 1
	*/
	return 0
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (bool, interface{}) {
	// Your code here.

	//px.mu.Lock()
	//defer px.mu.Unlock()

	//if px.Min() <= seq {
	decided, instance := px.getInstance(seq)
	return decided, instance.value
	//}
	//return false, nil

}

//
// tell the peer to shut itself down.
// for testing.
// please do not change this function.
//
func (px *Paxos) Kill() {
	px.dead = true
	if px.l != nil {
		px.l.Close()
	}
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me

	// Your initialization code here.
	px.slots = make(map[int]Instance)
	// My initialization

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.dead == false {
				conn, err := px.l.Accept()
				if err == nil && px.dead == false {
					if px.unreliable && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.unreliable && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						px.rpcCount++
						go rpcs.ServeConn(conn)
					} else {
						px.rpcCount++
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.dead == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}

	return px
}
