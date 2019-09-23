package main

import (
	"C"
	"errors"
	"fmt"
	"unsafe"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/fluent/fluent-bit-go/output"
)

type sqsConfig struct {
	queueURL string
	mySQS    *sqs.SQS
}

//export FLBPluginRegister
func FLBPluginRegister(def unsafe.Pointer) int {
	return output.FLBPluginRegister(def, "sqs", "aws sqs output plugin")
}

//export FLBPluginInit
func FLBPluginInit(plugin unsafe.Pointer) int {
	queueURL := output.FLBPluginConfigKey(plugin, "QueueUrl")
	writeInfoLog(fmt.Sprintf("queueURL is: %s", queueURL))

	if queueURL == "" {
		writeErrorLog(errors.New("QueueUrl configuration key is mandatory"))
		return output.FLB_ERROR
	}

	myAWSSession, err := session.NewSession()
	if err != nil {
		writeErrorLog(err)
		return output.FLB_ERROR
	}

	config := sqsConfig{
		queueURL: queueURL,
		mySQS:    sqs.New(myAWSSession),
	}

	// Set the context to point to any Go variable
	output.FLBPluginSetContext(plugin, config)

	return output.FLB_OK
}

//export FLBPluginFlushCtx
func FLBPluginFlushCtx(ctx, data unsafe.Pointer, length C.int, tag *C.char) int {
	// Type assert context back into the original type for the Go variable
	id := output.FLBPluginGetContext(ctx).(string)
	fmt.Printf("[multiinstance] Flush called for id: %s", id)
	return output.FLB_OK
}

//export FLBPluginExit
func FLBPluginExit() int {
	return output.FLB_OK
}

func writeInfoLog(message string) {
	fmt.Printf("[sqs-out] %s", message)
}

func writeErrorLog(err error) {
	fmt.Println("[sqs-out] ", err)
}

func main() {
}
