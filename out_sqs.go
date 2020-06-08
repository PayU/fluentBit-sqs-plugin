package main

import (
	"C"
	"errors"
	"fmt"
	"time"
	"unsafe"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/fluent/fluent-bit-go/output"
)
import (
	"encoding/json"
	"strings"
)

// MessageCounter is used for count the current SQS Batch messages
var MessageCounter int = 0

// SqsRecords is the actual aws messages batch
var SqsRecords []*sqs.SendMessageBatchRequestEntry

type sqsConfig struct {
	queueURL            string
	queueMessageGroupID string
	mySQS               *sqs.SQS
	pluginTagAttribute  string
}

//export FLBPluginRegister
func FLBPluginRegister(def unsafe.Pointer) int {
	return output.FLBPluginRegister(def, "sqs", "aws sqs output plugin")
}

//export FLBPluginInit
func FLBPluginInit(plugin unsafe.Pointer) int {
	queueURL := output.FLBPluginConfigKey(plugin, "QueueUrl")
	queueRegion := output.FLBPluginConfigKey(plugin, "QueueRegion")
	queueMessageGroupID := output.FLBPluginConfigKey(plugin, "QueueMessageGroupId")
	pluginTagAttribute := output.FLBPluginConfigKey(plugin, "PluginTagAttribute")
	writeInfoLog(fmt.Sprintf("QueueUrl is: %s", queueURL))
	writeInfoLog(fmt.Sprintf("QueueRegion is: %s", queueRegion))
	writeInfoLog(fmt.Sprintf("QueueMessageGroupId is: %s", queueMessageGroupID))
	writeInfoLog(fmt.Sprintf("pluginTagAttribute is: %s", pluginTagAttribute))

	if queueURL == "" {
		writeErrorLog(errors.New("QueueUrl configuration key is mandatory"))
		return output.FLB_ERROR
	}

	if queueRegion == "" {
		writeErrorLog(errors.New("QueueRegion configuration key is mandatory"))
		return output.FLB_ERROR
	}

	if strings.HasSuffix(queueURL, ".fifo") {
		if queueMessageGroupID == "" {
			writeErrorLog(errors.New("QueueMessageGroupId configuration key is mandatory for FIFO queues: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_SendMessage.html"))
			return output.FLB_ERROR
		}
	}

	writeInfoLog("retrieving aws credentials from environment variables")
	awsCredentials := credentials.NewEnvCredentials()
	var myAWSSession *session.Session
	var sessionError error

	// Retrieve the credentials value
	_, credError := awsCredentials.Get()
	if credError != nil {
		writeInfoLog("unable to find aws credentials from environment variables..using credentials chain")
		myAWSSession, sessionError = session.NewSession(&aws.Config{
			Region:                        aws.String(queueRegion),
			CredentialsChainVerboseErrors: aws.Bool(true),
		})
	} else {
		// environment variables credentials was found
		writeInfoLog("environment variables credentials where found")
		myAWSSession, sessionError = session.NewSession(&aws.Config{
			Region:                        aws.String(queueRegion),
			CredentialsChainVerboseErrors: aws.Bool(true),
			Credentials:                   awsCredentials,
		})
	}

	if sessionError != nil {
		writeErrorLog(sessionError)
		return output.FLB_ERROR
	}

	// Set the context to point to any Go variable
	output.FLBPluginSetContext(plugin, &sqsConfig{
		queueURL:            queueURL,
		queueMessageGroupID: queueMessageGroupID,
		mySQS:               sqs.New(myAWSSession),
		pluginTagAttribute:  pluginTagAttribute,
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

		if len(record) == 0 {
			writeInfoLog("got empty record from input. skipping it")
			continue
		}

		// Print record keys and values
		var timeStamp time.Time
		switch t := ts.(type) {
		case output.FLBTime:
			timeStamp = ts.(output.FLBTime).Time
		case uint64:
			timeStamp = time.Unix(int64(t), 0)
		default:
			writeInfoLog("given time is not in a known format, defaulting to now")
			timeStamp = time.Now()
		}

		tagStr := C.GoString(tag)
		recordString, err := createRecordString(timeStamp, tagStr, record)

		if err != nil {
			fmt.Printf("%v\n", err)
			// DO NOT RETURN HERE becase one message has an error when json is
			// generated, but a retry would fetch ALL messages again. instead an
			// error should be printed to console
			continue
		}

		MessageCounter++

		sqsRecord = &sqs.SendMessageBatchRequestEntry{
			Id:          aws.String(fmt.Sprintf("MessageNumber-%d", MessageCounter)),
			MessageBody: aws.String(recordString),
		}

		if sqsConf.pluginTagAttribute != "" {
			sqsRecord.MessageAttributes = map[string]*sqs.MessageAttributeValue{
				sqsConf.pluginTagAttribute: &sqs.MessageAttributeValue{
					DataType:    aws.String("String"),
					StringValue: aws.String(tagStr),
				},
			}
		}

		if sqsConf.queueMessageGroupID != "" {
			sqsRecord.MessageGroupId = aws.String(sqsConf.queueMessageGroupID)
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

func unBoxMap(innerRecord map[interface{}]interface{}) (map[string]interface{}, error) {
	myMap := make(map[string]interface{})
	for key, value := range innerRecord {
		switch t := value.(type) {
		case map[interface{}]interface{}:
			myInnerMap := value.(map[interface{}]interface{})
			goDown, err := unBoxMap(myInnerMap)
			if err != nil {
				writeErrorLog(fmt.Errorf("error calling unBoxMap recursively"))
				return myMap, err
			}
			for innerKey, innerValue := range goDown {
				myMap[innerKey] = innerValue
			}
		case []byte:
			// prevent encoding to base64
			myMap[key.(string)] = string(t)
		default:
			myMap[key.(string)] = value
		}
	}
	return myMap, nil
}

func createRecordString(timestamp time.Time, tag string, record map[interface{}]interface{}) (string, error) {
	m, err := unBoxMap(record)
	if err != nil {
		writeErrorLog(fmt.Errorf("error calling unBoxMap(record)"))
	}
	// convert timestamp to RFC3339Nano
	m["@timestamp"] = timestamp.UTC().Format(time.RFC3339Nano)
	fmt.Println(m)
	js, err := json.Marshal(m)
	if err != nil {
		writeErrorLog(fmt.Errorf("error creating message for sqs. tag: %s. error: %v", tag, err))
		return "", err
	}

	return string(js), nil
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
