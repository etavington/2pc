package main

import(
 "log"
 "net"
 "context"
 "strconv"

 "Server/GOCouchDBAPIs"

 "google.golang.org/grpc"
 pb"Server/grpc_2pc"

 "github.com/go-kivik/kivik/v3"
 _"github.com/go-kivik/couchdb/v3"

)
/*type CouchDBAccount struct {
        Id      string `json:"_id,omitempty"`
        Rev     string `json:"_rev,omitempty"`
        AccountId int64 `json:"account_id,omitempty"`
        Deposit int32    `json:"deposit,omitempty"`
}*/

type Server struct{
  pb.UnimplementedTwoPhaseCommitServiceServer
  client *kivik.Client
}

func (server *Server)CreateAccount(ctx context.Context,request *pb.CreateAccountRequest)(*pb.Response,error){
  msg,err :=GOCouchDBAPIs.CreateAccounts(1,server.client,"bank4",request.GetAccountId())
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
  msg, err:= GOCouchDBAPIs.DeleteAccount(request.GetAccountId(),server.client,"bank4")
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
  account,err := GOCouchDBAPIs.ReadAccount(request.GetAccountId(),server.client,"bank4")
  msg:= strconv.FormatInt(account.AccountId,10)+"\n"+strconv.FormatInt(int64(account.Deposit),10)
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
  msg,err:= GOCouchDBAPIs.UpdateAccount(request.GetAccountId(),server.client,"bank4",request.GetAmount())
  //msg:= strconv.FormatInt(account.AccountId,10)+"\n"+strconv.FormatInt(int64(account.Deposit),10)
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

func(server *Server)Reset(ctx context.Context,request *pb.ResetRequest)(*pb.Response,error){
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
  }
  //client.DBExists(context.TODO(),DBname)
  //client.Destory(context.TODO(),DBname)
  //client.CreateDB(context.TODO(),DBname)
  return &pb.Response{
    Msg: "Success",
  },nil
}
//------------------------------------------------------------------------------------------------
type Queue struct{
  request *pb.BeginTransactionRequest
  account GOCouchDBAPIs.CouchDBAccount
}
var queue[100000]Queue

func(server *Server)BeginTransaction(ctx context.Context,request *pb.BeginTransactionRequest)(*pb.Response,error){
//haven't completed lock yet
  var account GOCouchDBAPIs.CouchDBAccount
  account,err_read := GOCouchDBAPIs.ReadAccount(request.GetAccountId(),server.client,"bank4")
  if err_read !=nil{
    panic(err_read)
  }
  q := Queue{
    request: request,
    account: account,
  }
  queue[request.GetTransactionId()]=q
  if(account.Deposit+request.GetAmount()>=0){
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
  msg_update, err_update:= GOCouchDBAPIs.UpdateAccount(request.GetAccountId(),server.client,"bank4",queue[request.GetTransactionId()].account.Deposit+queue[request.GetTransactionId()].request.GetAmount())
  if err_update !=nil{
     panic(err_update)
  }
  queue[request.GetTransactionId()]=Queue{}
  msg_update = "Commit"
  return &pb.Response{
    Msg: msg_update,
  },nil
}

func(server *Server)Abort(ctx context.Context,request *pb.AbortRequest)(*pb.Response,error){
  queue[request.GetTransactionId()]=Queue{}
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

  server:= Server{
    client: client,
  }

  return server,nil
}

func main(){
/*  GOCouchDBAPIs.CreateDBs("bank4")
  for i=1;i<=10000;i++{
    GOCouchDBAPIs.CreateAccounts(1,"bank4",i)
  } */
  server, err:=NewServer()
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
