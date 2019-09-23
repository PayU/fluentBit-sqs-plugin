package main

import (
	"C"
	"unsafe"

	"github.com/fluent/fluent-bit-go/output"
)

// FLBPluginRegister Gets called only once when the plugin.so is loaded
func FLBPluginRegister(def unsafe.Pointer) int {
	return output.FLBPluginRegister(def, "sqs", "aws sqs output plugin")
}

// FLBPluginInit Gets called only once for each instance you have configured.
func FLBPluginInit(plugin unsafe.Pointer) int {
	// queueURL := output.FLBPluginConfigKey(plugin, "QueueURL")

	// if queueURL == "" {
	// 	return output.FLB_ERROR
	// }

	return output.FLB_OK
}

// FLBPluginFlushCtx Gets called with a batch of records to be written to an instance.
func FLBPluginFlushCtx(ctx, data unsafe.Pointer, length C.int, tag *C.char) int {
	return output.FLB_OK
}

// FLBPluginExit Gets called when shuting down fluntBit
func FLBPluginExit() int {
	return output.FLB_OK
}

func main() {
}
