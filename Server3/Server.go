package main

import(
 "log"
 "net"
 "sync"
 "context"
 "strconv"

 "Server/GOCouchDBAPIs"

 "google.golang.org/grpc"
 "github.com/golang/protobuf/ptypes/empty"
 pb"Server/grpc_2pc"

 "github.com/go-kivik/kivik/v3"
 _"github.com/go-kivik/couchdb/v3"

)

type CouchDBAccount struct{
  account GOCouchDBAPIs.CouchDBAccount
}

type Server struct{
  pb.UnimplementedTwoPhaseCommitServiceServer
  client *kivik.Client
  cache GOCouchDBAPIs.Cache
  accounts_lock map[int32]sync.Mutex
}
 

func (server *Server)CreateAccount(ctx context.Context,request *pb.CreateAccountRequest)(*pb.Response,error){
  msg,err :=GOCouchDBAPIs.CreateAccounts(1,server.client,"bank1",server.cache,request.GetAccountId())
  if err !=nil{
    return &pb.Response{
       Msg: msg,
    },err
  }else{
    return &pb.Response{
       Msg: msg,
    },nil
  }
}

func(server *Server)DeleteAccount(ctx context.Context,request *pb.DeleteAccountRequest)(*pb.Response,error){
  msg, err:= GOCouchDBAPIs.DeleteAccount(request.GetAccountId(),server.client,"bank1",server.cache)
  if err !=nil{
    return &pb.Response{
     Msg: msg,
    },err
  }else{
    return &pb.Response{
     Msg: msg,
    },nil
  }
}

func(server *Server)ReadAccount(ctx context.Context,request *pb.ReadAccountRequest)(*pb.Response,error){
  var account GOCouchDBAPIs.CouchDBAccount
  account,err := GOCouchDBAPIs.ReadAccount(request.GetAccountId(),server.client,"bank1")
  msg:= strconv.FormatInt(int64(account.AccountId),10)+"\n"+strconv.FormatInt(int64(account.Deposit),10)
  if err !=nil{
    return &pb.Response{
      Msg: msg,
    },err
  }else{
    return &pb.Response{
      Msg: msg,
    },nil
  }
}

func(server *Server)UpdateAccount(ctx context.Context,request *pb.UpdateAccountRequest)(*pb.Response,error){
  //var account GOCouchDBAPIs.CouchDBAccount
  msg,err:= GOCouchDBAPIs.UpdateAccount(request.GetAccountId(),server.client,"bank1",request.GetAmount(),server.cache)
  if err !=nil{
    return &pb.Response{
      Msg: msg,
    },err
  }else{
    return &pb.Response{
      Msg: msg,
    },nil
  }
}

func(server *Server)Reset(ctx context.Context,request *empty.Empty)(*pb.Response,error){
  client :=GOCouchDBAPIs.CreatekivikClient()
  defer client.Close(context.Background())
  var list []string
  list ,err:=client.AllDBs(context.TODO())
  if err !=nil{
    log.Fatalf("Fail to call AllDBs: %v",err)
  }
  for i:=2 ;i<len(list);i++{
    println(list[i])
    //client.Destory(context.TODO(),list[i])
    //server.Cache.Map1:=make(map[int32])
    //client.CreateDB(context.TODO(),list[i])
  }
  //client.DBExists(context.TODO(),DBname)
  //client.Destory(context.TODO(),DBname)
  //client.CreateDB(context.TODO(),DBname)
  return &pb.Response{
    Msg: "Success",
  },nil
}
//------------------------------------------------------------------------------------------------
func(server *Server)BeginTransaction(ctx context.Context,request *pb.BeginTransactionRequest)(*pb.Response,error){
  /*var account GOCouchDBAPIs.CouchDBAccount
  account,err_read := GOCouchDBAPIs.ReadAccount(request.GetAccountId(),server.client,"bank1")
  if err_read !=nil{
    panic(err_read)
  }*/
  /*account_cache := CouchDBAccount{
    account: account,
  }
  cache[request.GetAccountId()]=account_cache*/
  /*for {
    if(cache[request.GetAccountId()].Mu.TryLock()){
       break
    }
  }*/
  //server.Mu=server.cache.Map1[request.GetAccountId()].Mu
  server.Mu.Lock()
  if(server.cache.Map1[request.GetAccountId()].Deposit+request.GetAmount()>=0){  
    msg :="Legal"
    return &pb.Response{
      Msg: msg,
    },nil
  }else{
    msg:="Illegal"
    return &pb.Response{
      Msg: msg,
    },nil
  }
}

func(server *Server)Commit(ctx context.Context,request *pb.CommitRequest)(*pb.Response,error){
  msg_update, err_update:= GOCouchDBAPIs.UpdateAccount(request.GetAccountId(),server.client,"bank1",server.cache.Map1[request.GetAccountId()].Deposit+request.GetAmount(),server.cache)
  if err_update !=nil{
     panic(err_update)
  }
  msg_update = "Commit"
  //Mu=server.cache.Map1[request.GetAccountId()].Mu
  server.Mu.Unlock()
  return &pb.Response{
    Msg: msg_update,
  },nil
}

func(server *Server)Abort(ctx context.Context,request *pb.AbortRequest)(*pb.Response,error){
  //Mu=server.cache.Map1[request.GetAccountId()].Mu
  server.Mu.Unlock()
  return &pb.Response{
    Msg: "Abort",
  },nil
}

func NewServer()(Server,error){
  client, err:=kivik.New("couch","http://admin:t102260424@localhost:5984")
  if err !=nil{
    panic(err)
  }
  defer client.Close(context.Background())
  
  cache:=GOCouchDBAPIs.LoadCache()
  
  server:= Server{
    client: client,
    cache: cache,
  }
  return server,nil
}

func main(){
  server, err:=NewServer()
  GOCouchDBAPIs.CreateDBs("bank1")
  /*for key,value :=range server.cache.Map1{
    print(key," ",value.AccountId," ",value.Deposit,"\n")
  }*/
  //print(server.cache.Map1[15784715].Deposit,"\n")
  lis ,err:=net.Listen("tcp",":50051")
  if err !=nil{
    log.Fatalf("Fail to listen: %v",err)
  }
  grpcserver :=grpc.NewServer()
  pb.RegisterTwoPhaseCommitServiceServer(grpcserver,&server)

  if err :=grpcserver.Serve(lis);err !=nil{
	  log.Fatalf("fail to serve: %v", err)
  }
}
