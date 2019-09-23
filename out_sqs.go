package main

import (
	"C"
	"fmt"
	"unsafe"

	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/fluent/fluent-bit-go/output"
)

var mySQS *sqs.SQS

// FLBPluginRegister is called by fluentbit
func FLBPluginRegister(def unsafe.Pointer) int {
	return output.FLBPluginRegister(def, "sqs", "Sqs Output plugin")
}

// FLBPluginInit is called by fluentbit
// plugin (context) pointer to fluentbit context (state/ c code)
func FLBPluginInit(plugin unsafe.Pointer) int {
	queueURL := output.FLBPluginConfigKey(plugin, "QueueUrl")
	printLog(fmt.Sprintf("queueURL = %s", queueURL))

	if queueURL == "" {
		printLog("QueueUrl configuration is key mandatory")
		return output.FLB_ERROR
	}

	// Set the context with the queueURL
	// output.FLBPluginSetContext(plugin, queueURL)

	return output.FLB_OK
}

//export FLBPluginFlush
func FLBPluginFlush(data unsafe.Pointer, length C.int, tag *C.char) int {
	var count int
	var ret int
	var ts interface{}
	var record map[interface{}]interface{}

	// Create Fluent Bit decoder
	dec := output.NewDecoder(data, int(length))

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
		fmt.Printf("[%d] %s: [%s, {", count, C.GoString(tag),
			timestamp.String())
		for k, v := range record {
			fmt.Printf("\"%s\": %v, ", k, v)
		}
		fmt.Printf("}\n")
		count++
	}

	// Return options:
	//
	// output.FLB_OK    = data have been processed.
	// output.FLB_ERROR = unrecoverable error, do not try this again.
	// output.FLB_RETRY = retry to flush later.
	return output.FLB_OK
}

// FLBPluginExit is called by fluentBit when shuting down
func FLBPluginExit() int {
	return output.FLB_OK
}

func printLog(message string) {
	fmt.Printf("[sqs-output-plugin] %s'\n", message)
}

func main() {
}
