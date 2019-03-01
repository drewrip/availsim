package main

import(
	"fmt"
	"time"
	"runtime"
	"github.com/drewrip/availsim/raft"
	"github.com/fatih/color"
)

type Data struct{
	Size		int

	//In Milliseconds
	TotalTime	int64

	Trials		int
}

func main(){
	runtime.GOMAXPROCS(4)
	TRIALS:=200
	data:=make([]Data,0)
	dinghydata:=make([]Data,0)
	// Making Data points
	for i:=3; i<53; i+=2{
		data = append(data, Data{Size: i, TotalTime: 0, Trials: 0})
		dinghydata = append(dinghydata, Data{Size: i, TotalTime: 0, Trials: 0})
	}
	for x:=0; x<TRIALS; x++{
		ind:=0
		for i:=3; i<53; i+=2{
			color.Yellow("[TEST] Starting for n=%d\n", i)
			leaderChange := make(chan raft.ElectionTimePair, 2048)
			_,k := raft.NewCluster(i, 10 * time.Millisecond, leaderChange, false)
			time.Sleep(1000 * time.Millisecond)

			// Need to consume the change message sent during initialization
			<-leaderChange

			k<-struct{}{}
			start:=time.Now()
			var elTime raft.ElectionTimePair
			select{
			case elTime=<-leaderChange:
			case<-time.After(1 * time.Second):
				i-=2
				continue
			}
			end:=time.Now()
			totalEl:=end.Sub(start)
			el:=int64(elTime.End.Sub(elTime.Start))
			color.Red("Total Elapsed Time: %dns\n", totalEl)
			color.Red("Took: %dns\n", el)
			data[ind].TotalTime += int64(totalEl)
			data[ind].Trials++
			ind++
			time.Sleep(500 * time.Millisecond)
		}
	}

	for x:=0; x<TRIALS; x++{
		ind:=0
		for i:=3; i<53; i+=2{
			color.Yellow("[TEST] Starting for n=%d\n", i)
			leaderChange := make(chan raft.ElectionTimePair, 2048)
			_,k := raft.NewCluster(i, 10 * time.Millisecond, leaderChange, true)
			time.Sleep(1000 * time.Millisecond)

			// Need to consume the change message sent during initialization
			<-leaderChange

			k<-struct{}{}
			start:=time.Now()
			var elTime raft.ElectionTimePair
			select{
			case elTime=<-leaderChange:
			case<-time.After(1 * time.Second):
				i-=2
				continue
			}
			end:=time.Now()
			totalEl:=end.Sub(start)
			el:=int64(elTime.End.Sub(elTime.Start))
			color.Red("Total Elapsed Time: %dns\n", totalEl)
			color.Red("Took: %dns\n", el)
			dinghydata[ind].TotalTime += int64(totalEl)
			dinghydata[ind].Trials++
			ind++
			time.Sleep(500 * time.Millisecond)
		}
	}

	fmt.Printf("Vanilla Raft (Total Time):\n")
	for i:=0; i<len(data); i++{
		fmt.Printf("%d\t%d\n", data[i].Size, data[i].TotalTime/int64(data[i].Trials))
	}

	fmt.Printf("Dinghy Raft (Total Time):\n")
	for i:=0; i<len(dinghydata); i++{
		fmt.Printf("%d\t%d\n", dinghydata[i].Size, dinghydata[i].TotalTime/int64(dinghydata[i].Trials))
	}
}