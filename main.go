package main

import(
	//"fmt"
	"time"
	"github.com/drewrip/availsim/raft"
)

func main(){
	raft.NewCluster(3)
	time.Sleep(10 * time.Second)
}