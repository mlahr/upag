package cli

import (
	"encoding/json"
	"io"
)

type ErrorResponse struct {
	Error ErrorDetail `json:"error"`
}

type ErrorDetail struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func PrintJSON(w io.Writer, value any) error {
	encoder := json.NewEncoder(w)
	encoder.SetEscapeHTML(false)
	return encoder.Encode(value)
}

func PrintErrorJSON(w io.Writer, err error) error {
	return PrintJSON(w, ErrorResponse{Error: ErrorDetail{
		Code:    "command_failed",
		Message: err.Error(),
	}})
}
