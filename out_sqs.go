//go:build linux

package main

import (
	"C"
	"errors"
	"fmt"
	"os"
	"strconv"
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
	"net/http"
	"net/url"
	"strings"
)

// integer representation for this plugin log level
// 0 - debug
// 1 - info
// 2 - error
var sqsOutLogLevel int

// MessageCounter is used for count the current SQS Batch messages
var MessageCounter int = 0

// SqsRecords is the actual aws messages batch
var SqsRecords []*sqs.SendMessageBatchRequestEntry

type sqsConfig struct {
	endpoint            string
	queueURL            string
	queueMessageGroupID string
	mySQS               *sqs.SQS
	pluginTagAttribute  string
	proxyURL            string
	batchSize           int
}

//export FLBPluginRegister
func FLBPluginRegister(def unsafe.Pointer) int {
	setLogLevel()
	return output.FLBPluginRegister(def, "sqs", "aws sqs output plugin")
}

//export FLBPluginInit
func FLBPluginInit(plugin unsafe.Pointer) int {
	endpoint := output.FLBPluginConfigKey(plugin, "SQSEndpoint")
	queueURL := output.FLBPluginConfigKey(plugin, "QueueUrl")
	queueRegion := output.FLBPluginConfigKey(plugin, "QueueRegion")
	queueMessageGroupID := output.FLBPluginConfigKey(plugin, "QueueMessageGroupId")
	pluginTagAttribute := output.FLBPluginConfigKey(plugin, "PluginTagAttribute")
	proxyURL := output.FLBPluginConfigKey(plugin, "ProxyUrl")
	batchSizeString := output.FLBPluginConfigKey(plugin, "BatchSize")

	writeInfoLog(fmt.Sprintf("SQSEndpoint is: %s", endpoint))
	writeInfoLog(fmt.Sprintf("QueueUrl is: %s", queueURL))
	writeInfoLog(fmt.Sprintf("QueueRegion is: %s", queueRegion))
	writeInfoLog(fmt.Sprintf("QueueMessageGroupId is: %s", queueMessageGroupID))
	writeInfoLog(fmt.Sprintf("pluginTagAttribute is: %s", pluginTagAttribute))
	writeInfoLog(fmt.Sprintf("ProxyUrl is: %s", proxyURL))
	writeInfoLog(fmt.Sprintf("BatchSize is: %s", batchSizeString))

	if endpoint == "" {
		endpoint = fmt.Sprintf("sqs.%s.amazonaws.com", queueRegion)
		writeInfoLog(fmt.Sprintf("Using default regional AWS endpoint: %s", endpoint))
	}

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

	batchSize, err := strconv.Atoi(batchSizeString)
	if err != nil || (0 > batchSize && batchSize > 10) {
		writeErrorLog(errors.New("BatchSize should be integer value between 1 and 10"))
		return output.FLB_ERROR
	}

	writeInfoLog("retrieving aws credentials from environment variables")
	awsCredentials := credentials.NewEnvCredentials()
	var myAWSSession *session.Session
	var sessionError error
	var awsConfig *aws.Config

	// Retrieve the credentials value
	_, credError := awsCredentials.Get()
	if credError != nil {
		writeInfoLog("unable to find aws credentials from environment variables..using credentials chain")
		awsConfig = &aws.Config{
			Region:                        aws.String(queueRegion),
			CredentialsChainVerboseErrors: aws.Bool(true),
		}
	} else {
		writeInfoLog("environment variables credentials were found")
		awsConfig = &aws.Config{
			Region:                        aws.String(queueRegion),
			CredentialsChainVerboseErrors: aws.Bool(true),
			Credentials:                   awsCredentials,
    	Endpoint:                      aws.String(endpoint),
		}
	}

	// if proxy
	if proxyURL != "" {
		writeInfoLog("set http client struct on aws configuration since proxy url has been found")
		awsConfig.HTTPClient = &http.Client{
			Transport: &http.Transport{
				Proxy: func(*http.Request) (*url.URL, error) {
					return url.Parse(proxyURL) // Or your own implementation that decides a proxy based on the URL in the request
				},
			},
		}
	}

	// create the session
	myAWSSession, sessionError = session.NewSession(awsConfig)
	if sessionError != nil {
		writeErrorLog(sessionError)
		return output.FLB_ERROR
	}
	writeInfoLog("AWS session created")

	// Set the context to point to any Go variable
	output.FLBPluginSetContext(plugin, &sqsConfig{
		queueURL:            queueURL,
		queueMessageGroupID: queueMessageGroupID,
		mySQS:               sqs.New(myAWSSession),
		pluginTagAttribute:  pluginTagAttribute,
		batchSize:           batchSize,
	})

	writeInfoLog("Fluentbit context created")

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

		writeDebugLog(fmt.Sprintf("got new record from input. record length is: %d", len(record)))

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
			writeErrorLog(err)
			// DO NOT RETURN HERE becase one message has an error when json is
			// generated, but a retry would fetch ALL messages again. instead an
			// error should be printed to console
			continue
		}

		MessageCounter++

		writeDebugLog(fmt.Sprintf("record string: %s", recordString))
		writeDebugLog(fmt.Sprintf("message counter: %d", MessageCounter))

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

		if MessageCounter == sqsConf.batchSize {
			err := sendBatchToSqs(sqsConf, SqsRecords)

			SqsRecords = nil
			MessageCounter = 0

			if err != nil {
				writeErrorLog(err)
				return output.FLB_ERROR
			}
		}
	}

	if SqsRecords != nil {
		writeInfoLog(fmt.Sprintf("Flushing pending %d records", len(SqsRecords)))
		err := sendBatchToSqs(sqsConf, SqsRecords)
		if err != nil {
			writeErrorLog(err)
			return output.FLB_ERROR
		}
		SqsRecords = nil
		MessageCounter = 0
	}

	return output.FLB_OK
}

//export FLBPluginExit
func FLBPluginExit() int {
	writeInfoLog("Exiting plugin now.")
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

func createRecordString(timestamp time.Time, tag string, record map[interface{}]interface{}) (string, error) {
	m := make(map[string]interface{})
	// convert timestamp to RFC3339Nano
	m["@timestamp"] = timestamp.UTC().Format(time.RFC3339Nano)
	for k, v := range record {
		switch t := v.(type) {
		case []byte:
			// prevent encoding to base64
			m[k.(string)] = string(t)
		default:
			m[k.(string)] = v
		}
	}
	js, err := json.Marshal(m)
	if err != nil {
		writeErrorLog(fmt.Errorf("error creating message for sqs. tag: %s. error: %v", tag, err))
		return "", err
	}

	return string(js), nil
}

func writeDebugLog(message string) {
	if sqsOutLogLevel == 0 {
		currentTime := time.Now()
		fmt.Printf("[%s] [ debug] [sqs-out] %s\n", currentTime.Format("2006.01.02 15:04:05"), message)
	}
}

func writeInfoLog(message string) {
	if sqsOutLogLevel <= 1 {
		currentTime := time.Now()
		fmt.Printf("[%s] [ info] [sqs-out] %s\n", currentTime.Format("2006.01.02 15:04:05"), message)
	}
}

func writeErrorLog(err error) {
	if sqsOutLogLevel <= 2 {
		currentTime := time.Now()
		fmt.Printf("[%s] [ error] [sqs-out] %v\n", currentTime.Format("2006.01.02 15:04:05"), err)
	}
}

func setLogLevel() {
	logEnv := os.Getenv("SQS_OUT_LOG_LEVEL")

	switch strings.ToLower(logEnv) {
	case "debug":
		sqsOutLogLevel = 0
	case "info":
		sqsOutLogLevel = 1
	case "error":
		sqsOutLogLevel = 2
	default:
		sqsOutLogLevel = 1 // info
	}
}

func main() {
}
