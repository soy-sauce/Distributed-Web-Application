//Connie Zhou cz1529
//Parallel and Distributed Project

package main

import (
  "github.com/kataras/iris"
  "strconv"
  "flag"
  "fmt"
  "net"
  "os"
  "encoding/gob"
  "time"
  "strings"
)


//initialize variable for backend endpoint
var service = ""
var pingCounter=0
var backends := []string{}
//Struct to contain information for each task item
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


//addTask will send an Operation struct including a new task name and description to server
//Input: ctx
func addTask(ctx iris.Context){
  taskToAdd := Operation{ID: -1, Name: ctx.PostValue("name"), Description: ctx.PostValue("description"), OperationType: "add"}
  postServer(service, taskToAdd)
  time.Sleep(time.Second)
  localTasks:=requestServer(service)
  ctx.ViewData("Tasks", localTasks)
  ctx.View("index.html")
}

//deleteTask will send an ID using Operation struct to server
//input: ctx
func deleteTask(ctx iris.Context){
  toDelete, _ := strconv.Atoi(ctx.Params().Get("ID"))
  taskToDelete :=  Operation{ID: toDelete, Name: "", Description: "", OperationType: "delete"}
  postServer(service, taskToDelete)
  time.Sleep(time.Second)
  localTasks:=requestServer(service)
  ctx.ViewData("Tasks", localTasks)
  ctx.View("index.html")
}

//updateView will pull up new page to allow user to update a task that was previously selected
//input: ctx
func updateView(ctx iris.Context){
  toUpdate, _ := strconv.Atoi(ctx.Params().Get("ID"))
  time.Sleep(time.Second)
  localTasks:=requestServer(service)
  task, ok := localTasks[toUpdate]
  if ok {
    ctx.ViewData("Task", task)
    ctx.View("task.html")
  }

}

//updateTask will send the ID that needs to be updated and the new Name and new Description via an operation struct to the server
//input: ctx
func updateTask(ctx iris.Context){
  toUpdate, _ := strconv.Atoi(ctx.Params().Get("ID"))
  taskToUpdate := Operation{ID: toUpdate, Name: ctx.PostValue("name"), Description: ctx.PostValue("description"), OperationType: "update"}
  postServer(service, taskToUpdate)

  localTasks:=requestServer(service)
  ctx.ViewData("Tasks", localTasks)
  ctx.View("index.html")
}


//requestServer will set up a TCP connection with server to receive the list of Tasks
//input: Service name
//output: Map of Tasks
func requestServer(service string)(map[int]Task) {
  conn, err := net.Dial("tcp", service)
  if err != nil {
    fmt.Fprint(os.Stderr, "could not connect: ", err.Error())
    os.Exit(1)
  }
  defer conn.Close()
  var output map[int]Task
  goben := gob.NewDecoder(conn)
  goben.Decode(&output)
  return output
}

//postServer will set up a TCP connection with server to send data wrapped inside an Operation Struct
//input: service, data
func postServer(service string, data Operation) {
  conn, err := net.Dial("tcp", service)
  if err != nil {
    fmt.Fprint(os.Stderr, "could not connect: ", err.Error())
    os.Exit(1)
  }
  defer conn.Close()
  godec := gob.NewEncoder(conn)
  godec.Encode(data)
}

//ping is used for failure detection. It continuously sends messages to the backend using tcp and waits for a response
//A counter is used and wrapped inside an operation type and the backend will return the same number
func ping(conn net.Conn){
  defer conn.Close()
  for {
    var response = -1
    goben := gob.NewDecoder(conn)
    godec := gob.NewEncoder(conn)
    godec.Encode(Operation{ID: pingCounter, Name: "", Description: "", OperationType: "ping"})
    time.Sleep(time.Second)
    err := goben.Decode(&response)
    if err!= nil{
      fmt.Println("Detected failure on "+service+" at "+time.Now().Format(time.RFC850))
      //if current backend is down connect to a new random backend.
      service = "localhost:" + backend[rand.Int() % len(backends)]
    }
    pingCounter+=1
  }
}

func main() {
  //Command Line Flags
  listenPtr := flag.Int("listen", 8080, "Listen on Port:")
  backendPtr := flag.String("backend", ":8090.:8091,:8092", "Backends: ")
  //backendPtr := flag.String("backend", ":8091,:8092.:8093", "Backends: ")
  flag.Parse()

  //Set up service name
  backends = strings.Split(fmt.Sprint(*backendPtr), ",")
  //connect to random backend
  service = "localhost:" + backend[rand.Int() % len(backends)]

  //Set up go routine for failure detection
  d := net.Dialer{Timeout: 30*time.Second}
  conn, err := d.Dial("tcp", service)
  if err != nil{
  	fmt.Println("can't connect")
  }
  postServer(service, Operation{ID: -1, Name: ctx.PostValue("name"), Description: ctx.PostValue("description"), OperationType: "add"})
  postServer(service, Operation{ID: 3, Name: ctx.PostValue("name"), Description: ctx.PostValue("description"), OperationType: "delete"})
  go ping(conn)

  //Register structs for gob
  gob.Register(Operation{})
  gob.Register(Task{})




  app := iris.New()

  app.RegisterView(iris.HTML("./views", ".html").Reload(true))

  //Display localTasks received from server
  app.Get("/", func(ctx iris.Context) {
 //Request data from server to display
    localTasks:=requestServer(service)
    ctx.ViewData("Tasks", localTasks)
		ctx.View("index.html")
	})

  //Get and Post methods
  app.Post("/add", addTask)
  app.Get("/delete/{ID: string}", deleteTask)
  app.Get("/updateView/{ID: string}", updateView)
  app.Post("/updateTask/{ID: string}", updateTask)

  app.Run(iris.Addr(":" + fmt.Sprint(*listenPtr)))



}
