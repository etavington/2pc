package main

import(
  "log"
  "context"

  pb"Client/grpc_2pc"
  "google.golang.org/grpc"
  "github.com/golang/protobuf/ptypes/empty"

)
func CallCreateAccount(client pb.TwoPhaseCommitServiceClient,request *pb.CreateAccountRequest)(){
   res, err:=client.CreateAccount(context.Background(),request)
   if err!=nil{
     log.Fatalf("Fail to call CreateAccount: %v",err)
   }
   println(res.Msg)
}

func CallDeleteAccount(client pb.TwoPhaseCommitServiceClient,request *pb.DeleteAccountRequest)(){
  res, err:=client.DeleteAccount(context.Background(),request)
  if err!=nil{
    log.Fatalf("Fail to call DeleteAccount: %v",err)
  }
  println(res.Msg)
}

func CallReadAccount(client pb.TwoPhaseCommitServiceClient,request *pb.ReadAccountRequest)(){
  res, err:=client.ReadAccount(context.Background(),request)
  if err!=nil{
    log.Fatalf("Fail to call ReadAccount: %v",err)
  }
  println(res.Msg)
}

func CallUpdateAccount(client pb.TwoPhaseCommitServiceClient,request *pb.UpdateAccountRequest)(){
  res, err:=client.UpdateAccount(context.Background(),request)
  if err!=nil{
    log.Fatalf("Fail to call UpdateAccount: %v",err)
  }
  println(res.Msg)
}

func CallReset(client pb.TwoPhaseCommitServiceClient,request *empty.Empty)(){
  res, err:=client.Reset(context.Background(),request)
  if err!=nil{
    log.Fatalf("Fail to call Reset: %v",err)
  }
  println(res.Msg)
}

func CallTwoPhaseCommit(client pb.TwoPhaseCommitServiceClient,begintransaction *pb.BeginTransactionRequest)(){
   res_begin, err_begin := client.BeginTransaction(context.Background(),begintransaction)
   if err_begin !=nil{
    log.Fatalf("Fail to call BeginTransaction: %v",err_begin)
   }
   if res_begin.Msg== "Legal"{
     commit := &pb.CommitRequest{
       AccountId: begintransaction.GetAccountId(),
       Amount: begintransaction.GetAmount(),
     }
     res_commit,err_commit:=client.Commit(context.Background(),commit)
     if err_commit !=nil{
      log.Fatalf("Fail to call Commit: %v",err_commit)
     }
     println(res_commit.Msg)
   }else{
     abort := &pb.AbortRequest{
      AccountId: begintransaction.GetAccountId(),
     }
     res_abort,err_abort:=client.Abort(context.Background(),abort)
     if err_abort !=nil{
      log.Fatalf("Fail to call Abort: %v",err_abort)
     }
     println(res_abort.Msg)
   } 
   //println(res_begin.Msg)
}

func main(){
   conn, err :=grpc.Dial("34.80.195.25:50051",grpc.WithInsecure())
   if err !=nil{
     log.Fatalf("Fail to dial server: %v",err)
   }
   defer conn.Close()
   client :=pb.NewTwoPhaseCommitServiceClient(conn)
   for i:=0; i<1; i++{
    /*Account*/
     /*request := &pb.UpdateAccountRequest{
            ServerIp:    "123",
            AccountId:    1,
            Amount: 100,
          } */ 
     request2:= &pb.BeginTransactionRequest{
      AccountId: 1,
      Amount: -1000,    
     }     
     //CallCreateAccount(client,request)
     //CallUpdateAccount(client,request)
     //CallReadAccount(client,request)
     //CallDeleteAccount(client,request)
     //CallReset(client,request)
     CallTwoPhaseCommit(client,request2)
   }
}