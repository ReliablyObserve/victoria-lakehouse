package s3reader

import (
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	smithy "github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// IsNotFound decides on the S3 error an error carries and never on its text
// (issue #503: a request id or a key spelling "404" made a failing read look like
// a missing object, and the read answered 200 without it).
func TestIsNotFound_ClassifiesByTheS3Error(t *testing.T) {
	apiErr := func(code string) error { return &smithy.GenericAPIError{Code: code, Message: "404 not found"} }
	respErr := func(status int) error {
		return &smithyhttp.ResponseError{Response: &smithyhttp.Response{Response: &http.Response{StatusCode: status}}, Err: errors.New("request id 7A404C0FFEE00000")}
	}
	for _, tc := range []struct {
		name        string
		err         error
		gone, typed bool
	}{
		{"nil", nil, false, false},
		{"NoSuchKey type", &types.NoSuchKey{}, true, true},
		{"NotFound type", &types.NotFound{}, true, true},
		{"NoSuchKey code", apiErr("NoSuchKey"), true, true},
		{"NotFound code", apiErr("NotFound"), true, true},
		{"AccessDenied whose text says 404", apiErr("AccessDenied"), false, true},
		{"InternalError", apiErr("InternalError"), false, true},
		{"HTTP 404", respErr(404), true, true},
		{"HTTP 403 whose text says 404", respErr(403), false, true},
		{"HTTP 503", respErr(503), false, true},
		{"wrapped NoSuchKey", fmt.Errorf("s3 GetObject k: %w", &types.NoSuchKey{}), true, true},
		{"wrapped AccessDenied", fmt.Errorf("s3 GetObject 404.parquet: %w", apiErr("AccessDenied")), false, true},
		{"plain text 404", errors.New("HTTP 404"), false, false},
		{"plain io error", errors.New("unexpected EOF"), false, false},
	} {
		gone, typed := IsNotFound(tc.err)
		if gone != tc.gone || typed != tc.typed {
			t.Errorf("%s: IsNotFound = (gone %v, typed %v), want (%v, %v)", tc.name, gone, typed, tc.gone, tc.typed)
		}
	}
}
