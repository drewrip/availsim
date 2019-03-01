package raft

import(
	"time"
	"fmt"
	"math/rand"
)

const(
	defaultTimeout time.Duration = 500*time.Millisecond
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
	Install bool
	NewTimeout time.Duration
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

type ElectionTimePair struct {
	Start time.Time
	End time.Time
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
	KillChan chan struct{}
	Latency time.Duration
	LeaderAlert chan ElectionTimePair
	ETP ElectionTimePair
}

// NewCluster takes in the size of the cluster to create and returns references to the nodes
func NewCluster(z int, lat time.Duration, la chan ElectionTimePair, ping bool) ([]*Raft, chan struct{}) {
	c := make([]*Raft, 0)
	peers := make(map[string](RPCChan))
	var leaderKillChan chan struct{}
	// Creating the connections between each of the nodes
	for i:=0; i<z; i++{
		peers[fmt.Sprintf("n%d", i)] = RPCChan(make(chan RPC, 2048))
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
		var myKillChan chan struct{}
		myKillChan = nil
		if i == 0{
			stateForRaft = Leader
			myKillChan = make(chan struct{}, 2048)
			leaderKillChan = myKillChan
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
			KillChan:			myKillChan,
			Latency:			lat,
			LeaderAlert:		la,
			ETP:				ElectionTimePair{},
		}
		c = append(c, r)
	}
	time.Sleep(500 * time.Millisecond)
	StartLoops(c, ping)
	return c, leaderKillChan
}

func StartLoops(rs []*Raft, ping bool){
	for _,n := range rs{
		fmt.Printf("[INIT] Staring Raft Loop for %s\n", n.ID)
		go n.RaftHandler(ping)
	}
}

func randTimeout(random *rand.Rand, min time.Duration) time.Duration {
	return time.Duration(random.Intn(int(min))+int(min))
}

// This is the loop to handle raft's actions and different RPC communication
func (r *Raft) RaftHandler(ping bool){
    random := rand.New(rand.NewSource(time.Now().UnixNano()))
	for{
		switch r.State{

		case Leader:
			r.LeaderAlert<-r.ETP
			fmt.Printf("[STATE] %s: Now a Leader\n", r.ID)
			/*for k := range r.Peers{
				makeRPC(r.Peers[k], Ping{From: r.ID, Recv: false})
			}
			*/
			if ping{
				start := time.Now()
				for k := range r.Peers{
					makeRPC(r.Peers[k], Ping{From: r.ID, Install: false})
				}
				resps:=0
				for resps < len(r.Peers){
					x:=<-r.RecvChan
					_, ok := x.(Ping)
					if !ok{
						continue
					}
					resps++
				}
				el:=time.Since(start)

				for k := range r.Peers{
					makeRPC(r.Peers[k], Ping{From: r.ID, Install: false, NewTimeout: el*20})
				}

				fmt.Printf("[INIT] timeout established at %v\n", el*20)
			}

			ticker := time.NewTicker(r.HeartbeatTimeout/10).C
			LeaderLoop:
			for{
				select{
				case x:=<-r.RecvChan:
					time.Sleep(r.Latency)
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

				case <-r.KillChan:
					fmt.Printf("[DEBUG] %s: KILL\n", r.ID)
					return
				}
			}
			
		case Follower:
			fmt.Printf("[STATE] %s: Now a Follower\n", r.ID)
			FollowerLoop:
			for{
				time.Sleep(r.Latency)
				select{
					// Determines how to handle incoming RPC
					case m:=<-r.RecvChan:
						switch m.(type){

						// Handles Ping requests from leader
						case Ping:
							m, _ := m.(Ping)
							if m.Install{
								r.HeartbeatTimeout = m.NewTimeout
							} else {
								makeRPC(r.Peers[m.Sender()], Ping{From: r.ID, Recv: true})
							}
						// Handles Vote Requests from candidate
						case RequestVoteRequest:
							fmt.Printf("[RPC] %s: Received RequestVoteRequest\n", r.ID)
							if m.GetTerm() > r.Term && r.VotedFor == ""{
								makeRPC(r.Peers[m.Sender()], RequestVoteResponse{VoteGranted: true, Term: r.Term})
								r.Term = m.GetTerm()
								r.VotedFor = m.Sender()
								goto FollowerLoop
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
						r.ETP.Start = time.Now()
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
					break CandidateLoop
				case x:=<-r.RecvChan:
					time.Sleep(r.Latency)
					switch x.(type){
						case RequestVoteResponse:
							fmt.Printf("%s: Received RequestVoteResponse\n", r.ID)
							ballot, ok := x.(RequestVoteResponse)
							if !ok{
								fmt.Printf("[ERR] I don't understand this RPC\n")
							}
							if ballot.GetTerm() > r.Term{
								r.State = Follower
								break CandidateLoop
							}
							if ballot.VoteGranted{
								votes++
							}
							if votes>=r.QuorumSize{
								r.State = Leader
								r.ETP.End = time.Now()
								break CandidateLoop
							}
						case AppendEntries:
							fmt.Printf("[STATE] Candidate %s: received AppendEntries stepping down\n", r.ID)
							r.State = Follower
							break CandidateLoop
					}

					
				}
				fmt.Printf("%s: Votes=%d\n", r.ID, votes)
			}
		}
	}
}