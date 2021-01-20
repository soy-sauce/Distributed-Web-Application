//Connie Zhou cz1529
//Parallel and Distributed Project

package main

import (
  "flag"
  "fmt"
  "net"
  "os"
  "encoding/gob"
  "sync"
  "time"
  "rand"
)

//Task is an object consisting of an ID, name, and description
type Task struct {
  ID int
  Name string
  Description string
}


//Struct to be transported between front and back end to run add/delete/update.
//This struct can contain an ID, name, or description of a Task and what operation should be performed on it.
type Operation struct {
  ID int
  Name string
  Description string
  OperationType string
}

type Vote struct {
  Term int
  VoteVal bool
}

type AppendEntry struct{
  LogID Int
  Item Operation
}

var wg sync.WaitGroup
//Task List to store all tasks
var globalTasks = make(map[int]Task)
//Counter to be used to create IDs for new tasks
var globalCounter = 0
//Term Counter
var term=0
//peer Backends
var peers = []string{}
//counter id for commit messages
var commits = 0
//timer used for Raft for new elections
var timer := time.NewTimer(randomTime())
//log
var logCounter=0
var log = make(map[int]Operation)

func randomTime(){
  const maxTime = 5000
	const minTime = 2000
	return time.Duration(Rand.Intn(maxTime-minTime)+minTime) * time.Millisecond
}




//runRaftServer will run functions based on the nodes role (leader, follower, candidate)
func runRaftServer(){
  //set role of self, it was also be a follower as soon as it joins or rejoins the server
  var role="follower"
  timer = time.NewTimer(randomTime())

  for{
    //if timer reaches 0, request vote to be leader
    if(<-timer.C){
      role="candidate"
      requestVote()
    }
    if(role=="leader"){
      go taskResolver(queue)
      //only leader gives out heartbeats
      go heartbeats()
    }
    //if(role=="follower") {
      //concurrently handle each connection requests
    //}

    //all roles will handle connections because all roles can get requests from frontend.
    //Only followers will deal with heartbeats and elections
    go handleConnection(conn, queue)
  }
}


//requestVote uses global variable term and global variable peers
//requestVote connects to peers to send an operation of OperationType "election" and ID of the new term and waits for a qourum
//if qourum is met then node becomes leader
func requestVote(){
  var count=1
  conn1, err := net.Dial("tcp", "localhost:" + peers[0])
  if err != nil {
    fmt.Fprint(os.Stderr, "could not connect: ", err.Error())
  }
  conn2, err := net.Dial("tcp", "localhost:" + peers[1])
  if err != nil {
    fmt.Fprint(os.Stderr, "could not connect: ", err.Error())
  }
  defer conn1.close()
  defer conn2.close()

  goben1 := gob.NewEncoder(conn1)
  goben2 := gob.NewEncoder(conn2)
  gobdec1 := gob.NewDecoder(conn1)
  gobdec1 := gob.NewDecoder(conn2)

  term+=1
  var answer Vote
  goben1.Encode(Operation{ID: term, Name:"", Description:"", OperationType:"election"})
  goben2.Encode(Operation{ID: term, Name:"", Description:"", OperationType:"election"})
  gobdec1.Decode(answer)
  if(answer.ID==term & answer.VoteVal=true){
    count+=1
  }
  gobdec2.Decode(answer)
  if(answer.ID==term & answer.VoteVal=true){
    count+=1
  }
  if (count>=2){
    role="leader"
  } else {
    role="follower"
  }
}

//heartbeat uses global variable peers to connect to peer nodes and send an Operation of OperationType "heartbeat" every 1000ms
func heartbeats(){
  conn1, err := net.Dial("tcp", "localhost:" + peers[0])
  if err != nil {
    fmt.Fprint(os.Stderr, "could not connect: ", err.Error())
  }
  conn2, err := net.Dial("tcp", "localhost:" + peers[1])
  if err != nil {
    fmt.Fprint(os.Stderr, "could not connect: ", err.Error())
  }
  defer conn1.close()
  defer conn2.close()
  goben1 := gob.NewEncoder(conn1)
  goben2 := gob.NewEncoder(conn2)
  for{
    time.Sleep(1000)
    goben1.Encode(Operation{ID: "", Name:"", Description:"", OperationType:"heartbeat"})
    goben2.Encode(Operation{ID: "", Name:"", Description:"", OperationType:"heartbeat"})
  }
}


//Initialize Values for our task list
func initialValues(){
  globalTasks[globalCounter] = Task{ID: globalCounter, Name: "Do Distributed Systems HW", Description: "Project Part 2"}
  globalCounter+=1
  globalTasks[globalCounter] = Task{ID: globalCounter, Name: "Do laundry", Description: ""}
  globalCounter+=1
  globalTasks[globalCounter] = Task{ID: globalCounter, Name: "Buy Groceries", Description: "Eggs, Bread, Milk"}
  globalCounter+=1

}

//deleteTask deletes a task from task list using an ID received through an Operation struct
func deleteTask(task Operation){
  delete(globalTasks, task.ID)
  return
}

//updateTask updates a task from task list using an ID, new name, and new description received through an Operation struct
func updateTask(task Operation){
  globalTasks[task.ID]=Task{ID: task.ID, Name: task.Name, Description: task.Description}
  return
}

//addTask adds a task to task list using a name and description received through an Operation struct and generates an ID using globalCounter.
func addTask(task Operation){
  globalTasks[globalCounter]=Task{ID: globalCounter, Name: task.Name, Description: task.Description}
  globalCounter+=1
  return
}

func taskResolver(queue chan Operation){
  for item := range queue {
    //used to detect qourum with commits
    var count=1

    //send commit message to other backends
    conn1, err := net.Dial("tcp", "localhost:" + peers[0])
    if err != nil {
      fmt.Fprint(os.Stderr, "could not connect: ", err.Error())
    }
    conn2, err := net.Dial("tcp", "localhost:" + peers[1])
    if err != nil {
      fmt.Fprint(os.Stderr, "could not connect: ", err.Error())
    }

    defer conn1.close()
    defer conn2.close()

    goben1 := gob.NewEncoder(conn1)
    goben2 := gob.NewEncoder(conn2)
    gobdec1 := gob.NewEncoder(conn1)
    gobdec2 := gob.NewEncoder(conn2)

    var acknowledge int
    goben1.Encode(Operation{ID: commits, Name:"", Description:"", OperationType:"commit"})
    goben2.Encode(Operation{ID: commits, Name:"", Description:"", OperationType:"commit"})
    godec1.Decode(acknowledge)
    if (acknowledge==commits){
      count+=1
    }
    godec2.Decode(acknowledge)
    if (acknowledge==commits){
      count+=1
    }
    //if we receive qourum, go ahead and make the change
    if(count>=2){
      if(item.OperationType=="add"){
        addTask(item)
      }
      if(item.OperationType=="delete"){
        deleteTask(item)
      }
      if(item.OperationType=="update"){
        updateTask(item)
      }

      //send append entry to other peers so they can update their logs
      goben1.Encode(AppendEntry{LogID: logCounter, Item:item})
      goben2.Encode(AppendEntry{LogID: logCounter, Item:item})
      //insert into log
      log[logCounter]:=item
      logCounter+=1
      //reset count for next commits
      //increment commit ID for next commit
      count=1
      commits+=1
    }
  }
}

func handleConnection(conn net.Conn, queue chan Operation){
  fmt.Fprintln(os.Stderr, "Accepted connection from",conn.RemoteAddr())


  defer conn.Close()

  goben := gob.NewEncoder(conn)
  gobdec := gob.NewDecoder(conn)

  err := goben.Encode(globalTasks)
  if err!= nil{
    fmt.Println(err)
  }

  var newList Operation
  var entryVal AppendEntry

  //first decode for any appendEntry Values
  err = gobdec.Decode(&entryVal)
  if err != nil{
    fmt.Println(err)
  } else {
    //update log
    log[entryVal.LogID]=entryVal.Item
    globalTasks = make([]string, 0, len(m))
  }


  //Then decode for all operations
  err = gobdec.Decode(&newList)
  if err != nil{
    fmt.Println(err)
  } else {
      //If operation type is a ping, acknowledge the ping by senidng the message back to the client
      if(newList.OperationType=="heartbeat"){
	       goben.Encode(newList.ID)
         timer.Reset()
    }
    if(newList.OperationType=="election"){
      //check if vote request is greater than current recorded term. This is how we ensure we vote once per term
      if(newList.ID>term){
      goben.Encode(Vote{Term: newList.ID, VoteVal: true})
      term=newList.ID
      //will reset timer to new times every time there is a new election
      timer = time.NewTimer(randomTime())
    } else {
      goben.Encode(Vote{Term: newList.ID, VoteVal: false})
    }
      }
      if(newList.OperationType=="commit"){
        //acknowledge back with the ID
        goben.Encode(newList.ID)
      }
        } else{
    //If operation type is an add/update/delete of a task, put the item into a channel
    wg.Add(1)
    queue <- newList
    }
  }
  wg.Wait()
}

func main() {
  initialValues()

  //Register structs to gob
  gob.Register(Operation{})
  gob.Register(Task{})

  listenPtr := flag.String("listen", 8090, "Self:")
  backendPtr := flag.String("backend", ":8091,:8092", "Other backends: ")
  flag.Parse()

	ln, err := net.Listen("tcp", ":" + fmt.Sprint(*listenPtr))
  peers := strings.Split(fmt.Sprint(*backendPtr), ",")

	if err != nil {
		fmt.Println("Couldn't bind socket")
	}

  var queue = make(chan Operation, 100)

  //For incoming connections from clients, the server will either accept new data in the form of an Operation struct or send data as a map of Tasks.

  for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Fprint(os.Stderr, "Failed to accept")
		}
	defer conn.Close()
    //concurrently handle each connection requests
    //go handleConnection(conn, queue)
    go runRaftServer()

    //concurrently be resolving each item in the channel
    //go taskResolver(queue)
	}
}
