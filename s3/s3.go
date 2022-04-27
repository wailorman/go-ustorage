package s3

import (
	"io"
	"net/http"

	"github.com/pkg/errors"
	"github.com/wailorman/go-upstorage"
)

const StorageClaimKind = "s3"
const DefaultContentType = "application/octet-stream"

const downloadURLParamsKey = "downloadUrl"
const uploadURLParamsKey = "uploadUrl"

func wrapHTTPError(resp *http.Response) error {
	kylobytes := 1024
	if resp.ContentLength > 0 && resp.ContentLength < int64(2*kylobytes) {
		rawBody, _ := io.ReadAll(resp.Body)
		return errors.Wrapf(upstorage.ErrUnknown, "%s `%s`", resp.Status, string(rawBody))
	}

	return errors.Wrapf(upstorage.ErrUnknown, "HTTP %s", resp.Status)
}
