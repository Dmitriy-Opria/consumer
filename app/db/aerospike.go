package db

import (
	"bytes"
	"fmt"
	"time"

	as "golib/aerospike"
	"golib/logs"
)

func AerospikeSave(muidn string, udfArguments *bytes.Buffer, functionName string) {

	if !aerospikeClient.IsConnected() {
		time.Sleep(10 * time.Second)
	}

	err := aerospikeClient.ExecFunction(muidn, functionName, udfArguments.Bytes())
	if err != nil {
		conf := aerospikeClient.GetConfig()
		logs.Critical(fmt.Sprintf("Aerospike execute(%q.%q): %q", conf.PackageName, functionName, err.Error()))
	}
}

func GetAerospikeSession() as.ISession {
	return aerospikeClient
}
