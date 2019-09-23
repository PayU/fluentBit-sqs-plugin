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
import (
	"time"

	"github.com/aws/aws-sdk-go/aws"
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
	writeInfoLog(fmt.Sprintf("QueueUrl is: %s", queueURL))

	if queueURL == "" {
		writeErrorLog(errors.New("QueueUrl configuration key is mandatory"))
		return output.FLB_ERROR
	}

	myAWSSession, err := session.NewSession()
	if err != nil {
		writeErrorLog(err)
		return output.FLB_ERROR
	}

	// Set the context to point to any Go variable
	output.FLBPluginSetContext(plugin, &sqsConfig{
		queueURL: queueURL,
		mySQS:    sqs.New(myAWSSession),
	})

	return output.FLB_OK
}

//export FLBPluginFlushCtx
func FLBPluginFlushCtx(ctx, data unsafe.Pointer, length C.int, tag *C.char) int {
	var count int
	var ret int
	var ts interface{}
	var record map[interface{}]interface{}
	var sqsRecord *sqs.SendMessageBatchRequestEntry
	var sqsBatch sqs.SendMessageBatchInput
	var sqsRecords []*sqs.SendMessageBatchRequestEntry

	// Type assert context back into the original type for the Go variable
	sqsConf, ok := output.FLBPluginGetContext(ctx).(*sqsConfig)

	if !ok {
		writeErrorLog(errors.New("Unexpected error during get plugin context in flush function"))
		return output.FLB_ERROR
	}

	// Create Fluent Bit decoder
	dec := output.NewDecoder(data, int(length))
	sqsBatch = sqs.SendMessageBatchInput{}

	// Iterate Records
	count = 0
	for {
		// Extract Record
		ret, ts, record = output.GetRecord(dec)
		if ret != 0 {
			break
		}

		// Print record keys and values
		timestamp := ts.(output.FLBTime)
		recordString := fmt.Sprintf("{\"tag\":\"%s\", \"timestamp\":\"%s\",", C.GoString(tag),
			timestamp.String())

		writeInfoLog("first")
		writeInfoLog(recordString)

		for k, v := range record {
			recordString = recordString + fmt.Sprintf("\"%s\": %v, ", k, v)
		}
		recordString = recordString + fmt.Sprintf("}\n")

		writeInfoLog("second")
		writeInfoLog(recordString)

		sqsRecord = &sqs.SendMessageBatchRequestEntry{
			Id:          aws.String(fmt.Sprintf("Message No: %d", count)),
			MessageBody: aws.String(recordString),
		}
		sqsRecords = append(sqsRecords, sqsRecord)
		count++
		if count%10 == 0 {
			sqsBatch = sqs.SendMessageBatchInput{
				Entries:  sqsRecords,
				QueueUrl: aws.String(sqsConf.queueURL),
			}
			sqsConf.mySQS.SendMessageBatch(&sqsBatch)
			sqsRecords = nil
		}
	}

	return output.FLB_OK
}

//export FLBPluginExit
func FLBPluginExit() int {
	return output.FLB_OK
}

func writeInfoLog(message string) {
	currentTime := time.Now()
	fmt.Printf("[%s][info][sqs-out] %s\n", currentTime.Format("2006.01.02 15:04:05"), message)
}

func writeErrorLog(err error) {
	currentTime := time.Now()
	fmt.Printf("[%s][error][sqs-out] %v\n", currentTime.Format("2006.01.02 15:04:05"), err)
}

func main() {
}
