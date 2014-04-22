package paxos

type OpType int

const (
	NOOP OpType = iota
	GET
	PUT
	/*
		GetListOp
		PutToListOp
		AppendToListOp
	*/
)

type Instance struct {
	acceptedSeqNum int
	preparedSeqNum int
	isDecided      bool
	value          interface{}
}

type PrepareArgs struct {
	SeqNum  int
	SlotNum int
}

type PrepareReply struct {
	OK             bool
	AcceptedSeqNum int
	AcceptedValue  interface{}
	peerdone       int
	peerseq 	   int
}

type AcceptArgs struct {
	SeqNum      int
	SlotNum     int
	AcceptValue interface{}
}

type AcceptReply struct {
	AcceptedSeqNum int
	OK             bool
	peerdone       int
	peerseq 	   int
}

type DecideArgs struct {
	SeqNum       int
	SlotNum      int
	DecidedValue interface{}
}

type DecideReply struct {
}
