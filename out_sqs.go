package main

import (
	"C"
	"fmt"
	"unsafe"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/fluent/fluent-bit-go/output"
)

var mySQS *sqs.SQS
var QueueURL string

// FLBPluginRegister is called by fluentbit
func FLBPluginRegister(def unsafe.Pointer) int {
	return output.FLBPluginRegister(def, "sqs", "Aws SQS")
}

// FLBPluginInit is called by fluentbit
// plugin (context) pointer to fluentbit context (state/ c code)
func FLBPluginInit(plugin unsafe.Pointer) int {
	// Example to retrieve an optional configuration parameter
	QueueURL = output.FLBPluginConfigKey(plugin, "QueueUrl")
	fmt.Printf("[flb-go-sqs] plugin parameter = '%s'\n", QueueURL)

	mySession, err := session.NewSession()

	if err != nil {
		fmt.Println("Error", err)
		return output.FLB_ERROR
	}

	mySQS = sqs.New(mySession)
	return output.FLB_OK
}

//export FLBPluginFlush
func FLBPluginFlush(data unsafe.Pointer, length C.int, tag *C.char) int {
	var count int
	var ret int
	var ts interface{}
	var record map[interface{}]interface{}
	var sqsRecord *sqs.SendMessageBatchRequestEntry
	var sqsBatch sqs.SendMessageBatchInput
	var sqsRecords []*sqs.SendMessageBatchRequestEntry

	fmt.Printf("[flb-go-sqs] on FLBPluginFlush")

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
		for k, v := range record {
			recordString = recordString + fmt.Sprintf("\"%s\": %v, ", k, v)
		}
		recordString = recordString + fmt.Sprintf("}\n")

		sqsRecord = &sqs.SendMessageBatchRequestEntry{
			Id:          aws.String(fmt.Sprintf("Message No: %d", count)),
			MessageBody: aws.String(recordString),
		}
		sqsRecords = append(sqsRecords, sqsRecord)
		count++
		if count%10 == 0 {
			sqsBatch = sqs.SendMessageBatchInput{
				Entries:  sqsRecords,
				QueueUrl: aws.String(QueueURL),
			}
			mySQS.SendMessageBatch(&sqsBatch)
			sqsRecords = nil
		}
	}

	// Return options:
	//
	// output.FLB_OK    = data have been processed.
	// output.FLB_ERROR = unrecoverable error, do not try this again.
	// output.FLB_RETRY = retry to flush later.
	return output.FLB_OK
}

//export FLBPluginExit
func FLBPluginExit() int {
	return output.FLB_OK
}

func main() {
}
