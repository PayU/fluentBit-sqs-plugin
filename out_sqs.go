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
	return output.FLBPluginRegister(def, "sqs", "SQS")
}

// FLBPluginInit is called by fluentbit
// plugin (context) pointer to fluentbit context (state/ c code)
func FLBPluginInit(plugin unsafe.Pointer) int {
	queueURL := output.FLBPluginConfigKey(plugin, "QueueUrl")
	printLog(fmt.Sprintf("queueURL = %s", queueURL))

	// if queueURL == "" {
	// 	printLog("QueueUrl configuration is key mandatory")
	// 	return output.FLB_ERROR
	// }

	// Set the context with the queueURL
	output.FLBPluginSetContext(plugin, queueURL)

	return output.FLB_OK
}

// FLBPluginFlushCtx is called by fluentbit
func FLBPluginFlushCtx(ctx, data unsafe.Pointer, length C.int, tag *C.char) int {
	// Type assert context back into the original type for the Go variable
	queueURL := output.FLBPluginGetContext(ctx).(string)
	printLog(fmt.Sprintf("flash called. queueURL = %s", queueURL))

	dec := output.NewDecoder(data, int(length))

	count := 0
	for {
		ret, ts, record := output.GetRecord(dec)
		if ret != 0 {
			break
		}

		// Print record keys and values
		timestamp := ts.(output.FLBTime)
		fmt.Printf("[%d] %s: [%s, {", count, C.GoString(tag), timestamp.String())

		for k, v := range record {
			fmt.Printf("\"%s\": %v, ", k, v)
		}
		fmt.Printf("}\n")
		count++
	}

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
