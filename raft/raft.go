package raft

import(
	"time"
	"fmt"
	"math/rand"
)

const(
	defaultTimeout time.Duration = 500*time.Millisecond
	rpcLatency time.Duration = 10 * time.Millisecond
)

type RaftState byte

const(
	Leader RaftState = 0x00
	Follower RaftState = 0x11
	Candidate RaftState = 0x22
)

type RPC interface {
	Sender() string
	GetTerm() int
}

type RPCChan chan RPC

type Ping struct {
	From string
	Recv bool
	Term int
}


type RequestVoteRequest struct {
	// Arguments
	Term int
	From string
}

type RequestVoteResponse struct {
	// Returns
	VoteGranted bool
	Term int
	From string

}

type AppendEntries struct {
	Term int
	From string
	LeaderID string
}

func makeRPC(rpcChan RPCChan, rpc RPC){
	rpcChan<-rpc
}

func (p Ping) Sender() string {
	return p.From
}

func (p Ping) GetTerm() int {
	return p.Term
}

func (a AppendEntries) Sender() string {
	return a.From
}

func (a AppendEntries) GetTerm() int {
	return a.Term
}

func (v RequestVoteRequest) Sender() string {
	return v.From
}

func (v RequestVoteResponse) Sender() string {
	return v.From
}

func (v RequestVoteRequest) GetTerm() int {
	return v.Term
}

func (v RequestVoteResponse) GetTerm() int {
	return v.Term
}

// Main structure for Raft, holds and represents a Raft node
type Raft struct {
	HeartbeatTimeout time.Duration
	ID string
	Peers map[string](RPCChan)
	RecvChan RPCChan
	Term int
	VotedFor string
	State RaftState
	QuorumSize int
}

// NewCluster takes in the size of the cluster to create and returns references to the nodes
func NewCluster(z int) []*Raft {
	c := make([]*Raft, 0)

	peers := make(map[string](RPCChan))

	// Creating the connections between each of the nodes
	for i:=0; i<z; i++{
		peers[fmt.Sprintf("n%d", i)] = RPCChan(make(chan RPC, 1028))
	}

	// Creating the actuall Raft structs
	for i:=0; i<z; i++{
		myPeers := make(map[string](RPCChan))
		for k,v := range peers{
			if k != fmt.Sprintf("n%d", i){
				myPeers[k] = v
			}
		}
		// Ensures you aren't sending messages to yourself
		delete(myPeers, fmt.Sprintf("n%d", i))

		stateForRaft := Follower
		if i == 0{
			stateForRaft = Leader
		}
		r := &Raft{
			HeartbeatTimeout:	defaultTimeout,
			ID:					fmt.Sprintf("n%d", i),
			Peers:				myPeers,
			RecvChan:			peers[fmt.Sprintf("n%d", i)],
			Term:				0,
			VotedFor:			"",
			State:				stateForRaft,
			QuorumSize:			(z/2)+1,
		}
		c = append(c, r)
	}
	time.Sleep(1 * time.Second)
	StartLoops(c)
	return c
}

func StartLoops(rs []*Raft){
	fmt.Println(rs)
	for _,n := range rs{
		fmt.Printf("[INIT] Staring Raft Loop for %s\n", n.ID)
		go n.RaftHandler()
	}
}

func randTimeout(random *rand.Rand, min time.Duration) time.Duration {
	return time.Duration(random.Intn(int(min))+int(min))
}

// This is the loop to handle raft's actions and different RPC communication
func (r *Raft) RaftHandler(){
    random := rand.New(rand.NewSource(time.Now().UnixNano()))
    killTime:=time.After(3 * time.Second)
	for{
		switch r.State{

		case Leader:
			fmt.Printf("[STATE] %s: Now a Leader\n", r.ID)
			/*for k := range r.Peers{
				makeRPC(r.Peers[k], Ping{From: r.ID, Recv: false})
			}
			*/
			ticker := time.NewTicker(r.HeartbeatTimeout/10).C
			LeaderLoop:
			for{
				select{
				case x:=<-r.RecvChan:
					switch x.(type){
						case RequestVoteRequest:
							if x.GetTerm() > r.Term{
								r.State = Follower
								makeRPC(r.Peers[x.Sender()], RequestVoteResponse{VoteGranted: true, Term: r.Term})
								break LeaderLoop
							} else {
								makeRPC(r.Peers[x.Sender()], RequestVoteResponse{VoteGranted: false, Term: r.Term})
							}
						case AppendEntries:
							if x.GetTerm() > r.Term{
								r.State = Follower
								break LeaderLoop
							}
					}
				case <-ticker:
					for k := range r.Peers{
						makeRPC(r.Peers[k], AppendEntries{From: r.ID, Term: r.Term, LeaderID: r.ID})
					} 

				case <-killTime:
					return
				}
			}
			
		case Follower:
			fmt.Printf("[STATE] %s: Now a Follower\n", r.ID)
			FollowerLoop:
			for{
				select{
					// Determines how to handle incoming RPC
					case m:=<-r.RecvChan:
						switch m.(type){

						// Handles Ping requests from leader
						case Ping:
							makeRPC(r.Peers[m.Sender()], Ping{From: r.ID, Recv: true})

						// Handles Vote Requests from candidate
						case RequestVoteRequest:
							fmt.Printf("[RPC] %s: Received RequestVoteRequest\n", r.ID)
							if m.GetTerm() > r.Term && r.VotedFor == ""{
								makeRPC(r.Peers[m.Sender()], RequestVoteResponse{VoteGranted: true, Term: r.Term})
								r.Term = m.GetTerm()
								r.VotedFor = m.Sender()
							} else {
								makeRPC(r.Peers[m.Sender()], RequestVoteResponse{VoteGranted: false, Term: r.Term})
							}

						case AppendEntries:
							//fmt.Printf("[HB] %s\n", r.ID)
							if m.GetTerm() > r.Term{
								r.Term = m.GetTerm()
								r.VotedFor = ""
							}

						}

					case <-time.After(randTimeout(random, r.HeartbeatTimeout)):
						fmt.Printf("%s: heartbeat timeout\n", r.ID)
						r.State = Candidate
						break FollowerLoop

					
				}

			}

		case Candidate:
			r.Term++
			r.VotedFor = r.ID
			fmt.Printf("[STATE] %s: Now a Candidate\n", r.ID)
			votes:=1
			for k := range r.Peers{
				fmt.Printf("%v: Distributing ballot to %v\n", r.ID, k)
				makeRPC(r.Peers[k], RequestVoteRequest{From: r.ID, Term: r.Term})
			}

			CandidateLoop:
			for{
				select{
				case <-time.After(randTimeout(random, r.HeartbeatTimeout)):
					r.State = Follower
					break CandidateLoop
				case x:=<-r.RecvChan:
					fmt.Println(x)
					ballot, ok := x.(RequestVoteResponse)
					if !ok{
						fmt.Printf("[ERR] I don't understand this RPC\n")
						break CandidateLoop
					}
					fmt.Printf("%s: Received RequestVoteResponse\n", r.ID)
					if ballot.GetTerm() > r.Term{
						r.State = Follower
						break CandidateLoop
					}
					if ballot.VoteGranted{
						votes++
					}
					if votes>=r.QuorumSize{
						r.State = Leader
						break CandidateLoop
					}
					
				}
				fmt.Printf("%s: Votes=%d\n", r.ID, votes)
			}
		}
	}
}