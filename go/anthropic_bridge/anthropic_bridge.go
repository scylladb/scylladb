package main

/*
#include <stdlib.h>
#include <string.h>

typedef struct {
    char* response;
    char* error;
    int success;
} AnthropicResult;
*/
import "C"

import (
	"context"
	"fmt"
	"unsafe"

	"github.com/anthropics/anthropic-sdk-go"
	"github.com/anthropics/anthropic-sdk-go/option"
)

var (
	client *anthropic.Client
	apiKey string
)

//export anthropic_init
func anthropic_init(key *C.char) C.int {
	apiKey = C.GoString(key)
	if apiKey == "" {
		return 0
	}
	client = anthropic.NewClient(option.WithAPIKey(apiKey))
	return 1
}

//export anthropic_diagnose_error
func anthropic_diagnose_error(errorMsg *C.char, query *C.char, modelName *C.char) C.AnthropicResult {
	result := C.AnthropicResult{}

	if client == nil {
		result.error = C.CString("Anthropic client not initialized")
		result.success = 0
		return result
	}

	goError := C.GoString(errorMsg)
	goQuery := C.GoString(query)
	goModel := C.GoString(modelName)
	if goModel == "" {
		goModel = "claude-sonnet-4-20250514"
	}

	prompt := fmt.Sprintf(`You are an expert ScyllaDB/Cassandra database administrator.

A user encountered the following error while executing a CQL query.

Error Message: %s

Query: %s

Please diagnose this error and provide:
1. **Root Cause**: What went wrong
2. **Explanation**: Why this error occurred
3. **Solution**: Steps to fix the issue
4. **Corrected Query**: If applicable, provide a corrected version of the query

Be concise and practical.`, goError, goQuery)

	ctx := context.Background()
	message, err := client.Messages.New(ctx, anthropic.MessageNewParams{
		Model:     anthropic.F(goModel),
		MaxTokens: anthropic.Int(1024),
		Messages: anthropic.F([]anthropic.MessageParam{
			anthropic.NewUserMessage(anthropic.NewTextBlock(prompt)),
		}),
	})

	if err != nil {
		result.error = C.CString(fmt.Sprintf("API error: %v", err))
		result.success = 0
		return result
	}

	var responseText string
	for _, block := range message.Content {
		if block.Type == anthropic.ContentBlockTypeText {
			responseText += block.Text
		}
	}

	result.response = C.CString(responseText)
	result.success = 1
	return result
}

//export anthropic_free_result
func anthropic_free_result(result C.AnthropicResult) {
	if result.response != nil {
		C.free(unsafe.Pointer(result.response))
	}
	if result.error != nil {
		C.free(unsafe.Pointer(result.error))
	}
}

//export anthropic_is_initialized
func anthropic_is_initialized() C.int {
	if client != nil {
		return 1
	}
	return 0
}

func main() {}
