package main

import(
	"fmt"
	"time"
	"runtime"
	"github.com/drewrip/availsim/raft"
	"github.com/fatih/color"
	"flag"
	"database/sql"
    _ "github.com/go-sql-driver/mysql"
)

type Data struct{
	Size		int

	//In Milliseconds
	TotalTime	int64

	Trials		int
}

func check(err error){
	if err != nil {
        panic(err)
    }
}

func main(){
	// Flags
	var sqlServerAddr string
	var processes int
	var trials int
	var withDinghy bool
	flag.StringVar(&sqlServerAddr, "s", "", "address of the SQL database to pump results into. This assumes your database has a corresponding table for the test you wish to run, rafttimes for pure raft, and dinghytimes for raft with dinghy. Should be provided in the format: user:password@tcp(127.0.0.1:3306)/dbname")
	flag.IntVar(&processes, "j", 4, "(max) number of processes to run test with")
	flag.IntVar(&trials, "n", 1, "number of trials to run, one trial is testing all odd sizes 3-51")
	flag.BoolVar(&withDinghy, "d", false, "run with timeout retargeting algorithm Dinghy")
	flag.Parse()
	runtime.GOMAXPROCS(processes)
	tablename := "rafttimes"
	if withDinghy{
		tablename = "dinghytimes"
	}
		
	db, _ := sql.Open("mysql", sqlServerAddr) // Assumes MySQL db

	data:=make([]Data,0)
	// Making Data points
	for i:=3; i<53; i+=2{
		data = append(data, Data{Size: i, TotalTime: 0, Trials: 0})
	}
	for x:=0; x<trials; x++{
		ind:=0
		for i:=3; i<53; i+=2{
			color.Yellow("[TEST] Starting for n=%d\n", i)
			leaderChange := make(chan raft.ElectionTimePair, 2048)
			_,k := raft.NewCluster(i, 10 * time.Millisecond, leaderChange, withDinghy)
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
			totalEl:=int64(end.Sub(start))/1e6
			el:=int64(elTime.End.Sub(elTime.Start))/1e6
			color.Red("Total Elapsed Time: %dms\n", totalEl)
			color.Red("Took: %dms\n", el)

			// Process Results
			if sqlServerAddr != ""{
				_, err := db.Query(fmt.Sprintf("UPDATE %s SET time = time + %d, trials = trials + 1 WHERE size = %d", tablename, totalEl, i))
				check(err)
				color.Cyan("WROTE RESULTS TO DATABASE")
			}
			data[ind].TotalTime += totalEl
			data[ind].Trials++
			ind++
			time.Sleep(500 * time.Millisecond)
		}
	}

	fmt.Printf("Cluster Size:\tElection Time (ms):\n")
	for i:=0; i<len(data); i++{
		fmt.Printf("%d\t\t%d\n", data[i].Size, data[i].TotalTime/int64(data[i].Trials))
	}

}