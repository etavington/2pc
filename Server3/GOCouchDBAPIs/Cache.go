package GOCouchDBAPIs

import(
   "os"	
   "sync"
   "encoding/json"
)

type Cache struct{
   To_file map[int32]CouchDBAccount
   Local_accounts sync.Map       
}

func (cache *Cache)WriteToFile(To_file map[int32]CouchDBAccount)(error){
    file, err :=os.OpenFile("Cache/Cache.json",os.O_WRONLY|os.O_CREATE|os.O_TRUNC,0644)
    if err!=nil{
       return err
    }
    defer file.Close()

    encoder :=json.NewEncoder(file)
    if err:=encoder.Encode(To_file);err!=nil{
      return err
    }

    return nil
}

func (cache *Cache)ReadFromFile()(map[int32]CouchDBAccount,error){
     file, err:=os.Open("Cache/Cache.json")
     if err!=nil{
        return nil,err
     }
     defer file.Close()

     var cache_tmp Cache
     decoder:=json.NewDecoder(file)
     if err:=decoder.Decode(&cache_tmp.To_file);err!=nil{
         return nil, err
     }

     return cache_tmp.To_file,nil
}

func SyncMapToMap(Local_accounts sync.Map)map[int32]CouchDBAccount {
   To_file_tmp:=make(map[int32]CouchDBAccount)
   Local_accounts.Range(func (key,value interface{}) bool{
	  k,ok:=key.(int32)
	  if !ok{
		print("Type Error")
		return true
	  }
	  v,ok:=value.(CouchDBAccount)
	  if !ok{
		print("Type Error")
		return true
	  }
     To_file_tmp[k]=v
	  return true
   })
   return To_file_tmp
}

func MapToSyncMap(To_file map[int32]CouchDBAccount) sync.Map{
	 var Local_accounts_tmp sync.Map
     for k,v :=range To_file{
      Local_accounts_tmp.Store(k,v)
	 }
	 return Local_accounts_tmp
}