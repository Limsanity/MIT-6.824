package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	MaxTaskRunTime   = time.Second * 5
)

type Coordinator struct {
	// Your definitions here.
	taskPhase TaskPhase
	mutex sync.Mutex
	done bool

	files []string
	nReduce int
	nMap int
	
	taskCh chan Task
	taskStats []TaskStat
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) watchDog() {
	for !c.Done() {
		go func ()  {
			c.mutex.Lock()
			defer c.mutex.Unlock()

			for idx, taskStat := range c.taskStats {
				status := taskStat.status
				startTime := taskStat.startTime

				if status == Running && time.Since(startTime) > MaxTaskRunTime {
					fmt.Printf("[%-7s] [taskIdx: %d]\n", "timeout", idx)

					c.taskCh <- Task{
						Idx: idx,
						Filename: c.files[idx],
						Phase: c.taskPhase,
					}

					c.taskStats[idx].status = Pending
				}
			}	
		}()

		time.Sleep(MaxTaskRunTime)
	}
}

func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	task := <- c.taskCh

	c.mutex.Lock()
	defer c.mutex.Unlock()

	for task.Phase != c.taskPhase {
		task = <- c.taskCh
	}

	status := c.taskStats[task.Idx].status

	// 过滤超时重复任务
	if status == Error || status == Pending {
		reply.Task = &task
		reply.Phase = c.taskPhase
		reply.NReduce = c.nReduce
		reply.NMap = c.nMap
		c.taskStats[task.Idx].startTime = time.Now()
		c.taskStats[task.Idx].status = Running

		fmt.Printf("[%-7s] [taskIdx: %d]\n", "request", task.Idx)
	}

	return nil
}

func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if args.Phase != c.taskPhase {
		return nil
	}

	fmt.Printf("[%-7s] [taskIdx: %d] [status: %d]\n", "report", args.TaskIdx, args.Status)

	c.taskStats[args.TaskIdx].status = args.Status

	if args.Status == Error {
		c.taskCh <- Task{
			Idx: args.TaskIdx,
			Filename: c.files[args.TaskIdx],
		}
	}

	finish := true
	
	for _, stat := range c.taskStats {
		if stat.status != Finished {
			finish = false
		}
	}
	
	if finish {
		if c.taskPhase == MapPhase {
			fmt.Printf("----- reduce tasks ------\n")

			c.taskPhase = ReducePhase
			c.taskStats = c.taskStats[:0]
			for idx := 0; idx < c.nReduce; idx++ {
				c.taskCh <- Task{ Idx: idx, Phase: c.taskPhase }
				c.taskStats = append(c.taskStats, TaskStat{ status: Pending })
			}
		} else {
			c.done = true
		}
	}

	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}
//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.done
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// Your code here.
	c := Coordinator{
		done: false,
		mutex: sync.Mutex{},
		taskPhase: MapPhase,
		files: files,
		nMap: len(files),
		nReduce: nReduce,
	}

	c.taskCh = make(chan Task, c.nMap + c.nReduce)

	for idx := 0; idx < c.nMap; idx++ {
		c.taskCh <- Task{ Idx: idx, Filename: c.files[idx], Phase: c.taskPhase }
		c.taskStats = append(c.taskStats, TaskStat{ status: Pending })
	}

	go c.watchDog()

	c.server()
	return &c
}
