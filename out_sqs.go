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

// MessageCounter is used for count the current SQS Batch messages
var MessageCounter int = 0

// SqsRecords is the actual aws messages batch
var SqsRecords []*sqs.SendMessageBatchRequestEntry

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
	queueRegion := output.FLBPluginConfigKey(plugin, "QueueRegion")
	writeInfoLog(fmt.Sprintf("QueueUrl is: %s", queueURL))

	if queueURL == "" {
		writeErrorLog(errors.New("QueueUrl configuration key is mandatory"))
		return output.FLB_ERROR
	}

	if queueRegion == "" {
		writeErrorLog(errors.New("QueueRegion configuration key is mandatory"))
		return output.FLB_ERROR
	}

	myAWSSession, err := session.NewSession(&aws.Config{
		Region: aws.String(queueRegion),
	})

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
	var ret int
	var ts interface{}
	var record map[interface{}]interface{}
	var sqsRecord *sqs.SendMessageBatchRequestEntry

	// Type assert context back into the original type for the Go variable
	sqsConf, ok := output.FLBPluginGetContext(ctx).(*sqsConfig)

	if !ok {
		writeErrorLog(errors.New("Unexpected error during get plugin context in flush function"))
		return output.FLB_ERROR
	}

	// Create Fluent Bit decoder
	dec := output.NewDecoder(data, int(length))

	// Iterate Records
	for {
		// Extract Record
		ret, ts, record = output.GetRecord(dec)
		if ret != 0 {
			break
		}

		MessageCounter++
		writeInfoLog(fmt.Sprintf("count number is: %d", MessageCounter))

		// Print record keys and values
		timestamp := ts.(output.FLBTime)
		recordString := fmt.Sprintf("{\"tag\":\"%s\", \"timestamp\":\"%s\",", C.GoString(tag),
			timestamp.String())

		for k, v := range record {
			recordString = recordString + fmt.Sprintf("\"%s\": %v, ", k, v)
		}
		recordString = recordString + fmt.Sprintf("}\n")

		sqsRecord = &sqs.SendMessageBatchRequestEntry{
			Id:          aws.String(fmt.Sprintf("Message No: %d", MessageCounter)),
			MessageBody: aws.String(recordString),
		}

		SqsRecords = append(SqsRecords, sqsRecord)

		if MessageCounter == 10 {
			err := sendBatchToSqs(sqsConf, SqsRecords)

			if err != nil {
				writeErrorLog(err)
				return output.FLB_ERROR
			}

			SqsRecords = nil
			MessageCounter = 0
		}

	}

	return output.FLB_OK
}

//export FLBPluginExit
func FLBPluginExit() int {
	return output.FLB_OK
}

func sendBatchToSqs(sqsConf *sqsConfig, sqsRecords []*sqs.SendMessageBatchRequestEntry) error {
	writeInfoLog("sending batch messages to sqs..")
	sqsBatch := sqs.SendMessageBatchInput{
		Entries:  sqsRecords,
		QueueUrl: aws.String(sqsConf.queueURL),
	}

	output, err := sqsConf.mySQS.SendMessageBatch(&sqsBatch)

	if err != nil {
		return err
	}

	if len(output.Failed) > 0 {
		fmt.Println(output.Failed)
	}

	return nil
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
