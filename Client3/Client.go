package main

import(
  "log"
  "fmt"
  "sync"
  "time"
  "context"
  //"math/rand"

  pb"Client/grpc_2pc"
  "google.golang.org/grpc"
  "github.com/golang/protobuf/ptypes/empty"
)

var wg sync.WaitGroup

func CallCreateAccount(client pb.TwoPhaseCommitServiceClient,request *pb.CreateAccountRequest)(){
   res, err:=client.CreateAccount(context.Background(),request)
   if err!=nil{
     log.Fatalf("Fail to call CreateAccount: %v",err)
   }
   println(res.Msg)
   wg.Done()
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
  wg.Done()
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
   //println(res_begin.Msg)
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
     if res_abort.Msg =="Abort"{
        //println(res_abort.Msg)
     }
   } 
   wg.Done()
   //println(res_begin.Msg)
}

func main(){
   conn, err :=grpc.Dial("34.80.195.25:50051",grpc.WithInsecure())
   if err !=nil{
     log.Fatalf("Fail to dial server: %v",err)
   }
   defer conn.Close()
   client :=pb.NewTwoPhaseCommitServiceClient(conn)
   var i int32;
   startTime := time.Now()
   for i=1; i<=10000; i++{
    wg.Add(1)
    //rand_id:= rand.Int31()
    /*Account*/
    /*request := &pb.UpdateAccountRequest{
        AccountId: 77715539,
        Amount: 10000,
    }*/  
     /*request5:= &pb.CreateAccountRequest{
      AccountId: i,
     } */ 
     //go CallCreateAccount(client,request5)
     //go CallUpdateAccount(client,request)
     //CallReadAccount(client,request)
     //CallDeleteAccount(client,request)
     //CallReset(client,request)
     go func(){
      request2:= &pb.BeginTransactionRequest{
        AccountId: i,
        Amount: -10000000,    
       }
       /*request3:= &pb.BeginTransactionRequest{
        AccountId: 1778825488,
        Amount: -1,    
       }
       request4:= &pb.BeginTransactionRequest{
        AccountId: 1144598379,
        Amount: -1,    
       }*/
       CallTwoPhaseCommit(client,request2)
       /*if(i%3==0){
         CallTwoPhaseCommit(client,request2)
       }else if(i%3==1){
         CallTwoPhaseCommit(client,request3)
       }else{
         CallTwoPhaseCommit(client,request4)
       }*/
     }()
     time.Sleep(1*time.Microsecond)
   }
   wg.Wait()
   endTime := time.Now()
   diff := endTime.Sub(startTime)
   rate := float64(10000)/diff.Seconds() 
	 fmt.Println("rate:", rate)
 }
