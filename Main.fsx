#r "nuget: Akka.FSharp" 
#r "nuget: Akka.TestKit" 

open System
open Akka.Actor
open Akka.Configuration
open Akka.FSharp
open System.Diagnostics
open System.Collections.Generic




//____________________________________________________________________________________________
type GossipMessageTypes =
    | Initailize of IActorRef []
    | InitializeVariables of int
    | StartGossip of String
    | MsgReceived of String
    | StartPushSum of Double
    | ComputePushSum of Double * Double * Double
    | Result of Double * Double
    | Time of int
    | TotalNodes of int
    | ActivateGossipWorker 
    | WorkerActorCall
    | AddNeighbors

type Topology = 
    | Gossip of String
    | PushSum of String

type Protocol = 
    | Line of String
    | Full of String
    | TwoDimension of String

//__________________ Global Variables _______________________________________________________________
let mutable numNodes = int (string (fsi.CommandLineArgs.GetValue 1))
let topology = string (fsi.CommandLineArgs.GetValue 2)
let algo = string (fsi.CommandLineArgs.GetValue 3)
let timer = Diagnostics.Stopwatch()
let MyActorSystem = ActorSystem.Create("System")
let mutable  originlNodes = [||]

//__________________ Boss Actor _______________________________________________________________
let BossActor(mailbox: Actor<_>) =
    
    let mutable ctr = 0
    let mutable strtime = 0
    let mutable totalNodes = 0

    let rec loop()= actor{
        let! msg = mailbox.Receive();
        match msg with 
        | MsgReceived _ -> 
            let ending = DateTime.Now.TimeOfDay.Milliseconds
            ctr <- ctr + 1
            if ctr = totalNodes then
                timer.Stop()
                printfn "Time for convergence: %f ms" timer.Elapsed.TotalMilliseconds
                Environment.Exit(0)
        | Result (sum, weight) ->
            ctr <- ctr + 1
            if ctr = totalNodes then
                timer.Stop()
                
                printfn "Time for convergence: %f ms" timer.Elapsed.TotalMilliseconds
                Environment.Exit(0)
        | Time strtTime -> strtime <- strtTime
        | TotalNodes n -> totalNodes <- n
        | _ -> ()

        return! loop()
    }            
    loop()



//__________________ Boss Actor Spawn _______________________________________________________________
let Boss = spawn MyActorSystem "BossActor" BossActor
let checkNbrDict = new Dictionary<IActorRef, bool>()


//__________________ Worker Actor ____________________________________________________________________
let WorkerActor(mailbox: Actor<_>) =
    let mutable ctr = 0
    let mutable neighboursArr: IActorRef [] = [||]
    let mutable ttl = 0 |>double
    let mutable wgt = 1.0
    let mutable trounf = 1
    let mutable hasCnvrged = false
    
    
    let rec loop()= actor{
        let! msg = mailbox.Receive();
        
        match msg with 

        | Initailize reference ->
            neighboursArr <- reference

        | ActivateGossipWorker ->
            if ctr < 11 then
                let rnd = Random().Next(0, neighboursArr.Length)
                if not checkNbrDict.[neighboursArr.[rnd]] then
                    neighboursArr.[rnd] <! WorkerActorCall
                mailbox.Self <! ActivateGossipWorker

        | WorkerActorCall ->
            
            if ctr = 0 then 
                mailbox.Self <! ActivateGossipWorker
            if (ctr = 10) then 
                Boss <! MsgReceived "Rumor"
                checkNbrDict.[mailbox.Self] <- true
            ctr <- ctr + 1
            
        | InitializeVariables number ->
            ttl <- number |> double

        | StartPushSum delta ->
            let index = Random().Next(0, neighboursArr.Length)

            ttl <- ttl / 2.0
            wgt <- wgt / 2.0
            neighboursArr.[index] <! ComputePushSum(ttl, wgt, delta)

        | ComputePushSum (s: float, w, delta) ->
            let newsum = ttl + s
            let newweight = wgt + w
            let value = ttl / wgt - newsum / newweight |> abs

            if hasCnvrged then

                let index = Random().Next(0, neighboursArr.Length)
                neighboursArr.[index] <! ComputePushSum(s, w, delta)
            
            else
                if value > delta then
                    trounf <- 0
                else 
                    trounf <- trounf + 1

                if  trounf = 3 then
                    trounf <- 0
                    hasCnvrged <- true
                    Boss <! Result(ttl, wgt)
            
                ttl <- newsum / 2.0
                wgt <- newweight / 2.0
                let index = Random().Next(0, neighboursArr.Length)
                neighboursArr.[index] <! ComputePushSum(ttl, wgt, delta)
        | _ -> ()
        return! loop()
    }            
    loop()



let HelperActor (mailbox: Actor<_>) =
    let neighbors = new List<IActorRef>()
    let rec loop() = actor {
        let! message = mailbox.Receive()
        match message with 
        | AddNeighbors _ ->
            for i in [0..numNodes-1] do
                    neighbors.Add originlNodes.[i]
            mailbox.Self <! ActivateGossipWorker
        | ActivateGossipWorker ->
            if neighbors.Count > 0 then
                printfn "in inside Activateworker"
                let randomNumber = Random().Next(neighbors.Count)
                let randomActor = neighbors.[randomNumber]
                
                if (checkNbrDict.[neighbors.[randomNumber]]) then  
                    (neighbors.Remove randomActor) |>ignore
                else 
                    randomActor <! WorkerActorCall
                mailbox.Self <! ActivateGossipWorker 
        | _ -> ()
        return! loop()
    }
    loop()

//__________________ Worker Actor Spawn ________________________________________________________________

let Worker = spawn MyActorSystem "HelperActor" HelperActor

//__________________ Topologies _________________________________________________________________________
match topology with
| "line" ->
    originlNodes <- Array.zeroCreate (numNodes + 1)
    
    for x in [0..numNodes] do
        let key: string = "node" + string(x) 
        let actorRef = spawn MyActorSystem (key) WorkerActor
        originlNodes.[x] <- actorRef 
        checkNbrDict.Add(originlNodes.[x], false)
        originlNodes.[x] <! InitializeVariables x

    for i in [ 0 .. numNodes ] do
        let mutable neighbourRefArray = [||]
        if i = 0 then
            neighbourRefArray <- (Array.append neighbourRefArray [|originlNodes.[i+1]|])
        elif i = numNodes then
            neighbourRefArray <- (Array.append neighbourRefArray [|originlNodes.[i-1]|])
        else 
            neighbourRefArray <- (Array.append neighbourRefArray [| originlNodes.[(i - 1)] ; originlNodes.[(i + 1 ) ] |] ) 
        
        originlNodes.[i] <! Initailize(neighbourRefArray)

    let startNode = Random().Next(0, numNodes)
   
    timer.Start()
    match algo with
    | "gossip" -> 
        Boss <! TotalNodes(numNodes)
        Boss <! Time(DateTime.Now.TimeOfDay.Milliseconds)
        printfn "Starting Gossip"
        originlNodes.[startNode] <! ActivateGossipWorker
        Worker<! AddNeighbors
        
    | "push-sum" -> 
        Boss <! TotalNodes(numNodes)
        Boss <! Time(DateTime.Now.TimeOfDay.Milliseconds)
        printfn "Starting Push Sum "
        originlNodes.[startNode] <! StartPushSum(10.0 ** -10.0)     
    | _ ->
        printfn "Please provide One of the following Algorithms - "
        printfn " gossip "
        printfn " push-sum "  

| ("full" | "3d" | "imperfect 3d") ->
    originlNodes <- Array.zeroCreate (numNodes + 1)
    
    for x in [0..numNodes] do
        let key: string = "node" + string(x) 
        let actorRef = spawn MyActorSystem (key) WorkerActor
        originlNodes.[x] <- actorRef 
        originlNodes.[x] <! InitializeVariables x
        checkNbrDict.Add(originlNodes.[x], false)
    
    for i in [ 0 .. numNodes ] do
        let mutable neighbourRefArray = [||]
        for j in [0..numNodes] do 
            if i <> j then
                neighbourRefArray <- (Array.append neighbourRefArray [|originlNodes.[j]|])
        originlNodes.[i]<! Initailize(neighbourRefArray)
              

    timer.Start()

    let startNode = Random().Next(0, numNodes)

    match algo with
    | "gossip" -> 
        Boss <! TotalNodes(numNodes)
        Boss <! Time(DateTime.Now.TimeOfDay.Milliseconds)
        printfn "Starting Gossip"
        originlNodes.[startNode] <! WorkerActorCall
    | "push-sum" -> 
        Boss <! TotalNodes(numNodes)
        Boss <! Time(DateTime.Now.TimeOfDay.Milliseconds)
        printfn "Starting Push Sum"
        originlNodes.[startNode] <! StartPushSum(10.0 ** -10.0) 
    | _ ->
        printfn "Please provide One of the following Algorithms - "
        printfn " gossip "
        printfn " push-sum "

| "3dd" ->
    printfn "in 3D"
    originlNodes <- Array.zeroCreate (numNodes + 1)
    
    for x in [0..numNodes] do
        let key: string = "node" + string(x) 
        let actorRef = spawn MyActorSystem (key) WorkerActor
        originlNodes.[x] <- actorRef 
        originlNodes.[x] <! InitializeVariables x
        checkNbrDict.Add(originlNodes.[x], false)

    printfn "actor loop executed"
    
    let gridSize = int(ceil ((float numNodes)** (1.0/3.0)))
    let numNodein3D = gridSize * gridSize * gridSize
    let mutable NumLayer = gridSize

    for numN in [0..numNodein3D-1] do
        let mutable neighbourRef = [||]
        if (numN + 1 < numNodein3D) then
            printfn "in inside numN1"
            neighbourRef <- (Array.append neighbourRef [|originlNodes.[numN + 1]|])
        if (numN + NumLayer < numNodein3D) then
            printfn "in inside numN2"
            neighbourRef <- (Array.append neighbourRef [|originlNodes.[numN + NumLayer]|])
        if (numN + (NumLayer*NumLayer) < numNodein3D) then
            printfn "in inside numN3"
            neighbourRef <- (Array.append neighbourRef [|originlNodes.[numN + (NumLayer*NumLayer)]|])
        if (numN - 1 >= 0) then
            printfn "in inside numN4"
            neighbourRef <- (Array.append neighbourRef [|originlNodes.[numN - 1]|])
        if (numN - NumLayer >= 0) then
            printfn "in inside numN5"
            neighbourRef <- (Array.append neighbourRef [|originlNodes.[numN - NumLayer]|])
        if (numN - (NumLayer*NumLayer) > 0) then
            printfn "in inside numN6"
            neighbourRef <- (Array.append neighbourRef [|originlNodes.[numN - (NumLayer*NumLayer)]|])
       
        originlNodes.[numN] <! Initailize(neighbourRef)
    printfn "outside the loop"

    let startNode = Random().Next(0, numNodes)

    timer.Start()
    match algo with
    | "gossip" -> 
        printfn(" in gossip")
        Boss <! TotalNodes(numNodes)
        Boss <! Time(DateTime.Now.TimeOfDay.Milliseconds)
        printfn "Starting Gossip Algorithm"
        originlNodes.[startNode] <! ActivateGossipWorker
    | "push-sum" -> 
        Boss <! TotalNodes(numNodes)
        Boss <! Time(DateTime.Now.TimeOfDay.Milliseconds)
        printfn "Starting Push Sum"
        originlNodes.[startNode] <! StartPushSum(10.0 ** -10.0)   
    | _ ->
        printfn "Please provide One of the following Algorithms - "
        printfn " gossip "
        printfn " push-sum "


| "Imprfetc 3D" ->

    originlNodes <- Array.zeroCreate (numNodes + 1)
    
    for x in [0..numNodes] do
        let key: string = "node" + string(x) 
        let actorRef = spawn MyActorSystem (key) WorkerActor
        originlNodes.[x] <- actorRef 
        originlNodes.[x] <! InitializeVariables x
        checkNbrDict.Add(originlNodes.[x], false)

    
    let gridSize = int(ceil ((float numNodes)** (1.0/3.0)))
    let numNodein3D = gridSize * gridSize * gridSize
    let mutable NumLayer = gridSize

    for numN in [0..numNodein3D-1] do
        let mutable neighbourRef = [||]
        if (numN + 1 < numNodein3D) then
            neighbourRef <- (Array.append neighbourRef [|originlNodes.[numN + 1]|])
        if (numN + NumLayer < numNodein3D) then
            neighbourRef <- (Array.append neighbourRef [|originlNodes.[numN + NumLayer]|])
        if (numN + (NumLayer*NumLayer) < numNodein3D) then
            neighbourRef <- (Array.append neighbourRef [|originlNodes.[numN + (NumLayer*NumLayer)]|])
        if (numN - 1 >= 0) then
            neighbourRef <- (Array.append neighbourRef [|originlNodes.[numN - 1]|])
        if (numN - NumLayer >= 0) then
            neighbourRef <- (Array.append neighbourRef [|originlNodes.[numN - NumLayer]|])
        if (numN - (NumLayer*NumLayer) > 0) then
            neighbourRef <- (Array.append neighbourRef [|originlNodes.[numN - (NumLayer*NumLayer)]|])
        let rndneighbor = Random().Next(0, numNodes)
        neighbourRef <- (Array.append neighbourRef [|originlNodes.[rndneighbor]|])
        
        originlNodes.[numN] <! Initailize(neighbourRef)

    let startNode = Random().Next(1, numNodes)

    timer.Start()
    match algo with
    | "gossip" -> 
        printfn(" in gossip")
        Boss <! TotalNodes(numNodes)
        Boss <! Time(DateTime.Now.TimeOfDay.Milliseconds)
        printfn "Starting Gossip Algorithm"
        originlNodes.[startNode] <! ActivateGossipWorker
    | "push-sum" -> 
        Boss <! TotalNodes(numNodes)
        Boss <! Time(DateTime.Now.TimeOfDay.Milliseconds)
        printfn "Starting Push Sum"
        originlNodes.[startNode] <! StartPushSum(10.0 ** -10.0)  
    | _ ->
        printfn "Please provide One of the following Algorithms - "
        printfn " gossip "
        printfn " push-sum "


| _ ->
    printfn " Please provide One of the following Topologies - "
    printfn " imperfect 3d "
    printfn " 3d "
    printfn " full "
    printfn " line "

Console.ReadLine() |> ignore
