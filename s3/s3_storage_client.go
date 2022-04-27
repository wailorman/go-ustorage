package s3

import (
	"bytes"
	"context"
	"io"
	"net/http"

	"github.com/pkg/errors"
	"github.com/wailorman/go-filh"
	"github.com/wailorman/go-upstorage"
)

type StorageClient struct {
}

func NewStorageClient() *StorageClient {
	return &StorageClient{}
}

func (client *StorageClient) Download(ctx context.Context, sc *upstorage.StorageClaim, file filh.Filer, p chan upstorage.Progresser) error {
	// TODO: notify progress
	// TODO: handle ctx cancel

	if sc.Params[downloadURLParamsKey] == "" {
		return upstorage.ErrUnableDownload
	}

	if err := file.Create(); err != nil {
		return errors.Wrap(err, "Creating & truncating file for storage claim downloading")
	}

	writer, err := file.WriteContent()

	if err != nil {
		return errors.Wrap(err, "Building file writer")
	}

	defer writer.Close()

	res, err := http.Get(sc.Params[downloadURLParamsKey])

	if err != nil {
		return errors.Wrap(err, "Performing download request")
	}

	defer res.Body.Close()

	if res.StatusCode >= 400 {
		return errors.Wrapf(wrapHTTPError(res), "Failed download request (url: `%s`)", sc.Params[downloadURLParamsKey])
	}

	_, err = io.Copy(writer, res.Body)

	if err != nil {
		return errors.Wrap(err, "Writing response to writer")
	}

	return nil
}

func (client *StorageClient) Upload(ctx context.Context, sc *upstorage.StorageClaim, file filh.Filer, p chan upstorage.Progresser) error {
	// TODO: notify progress
	// TODO: handle ctx cancel

	if !file.IsExist() {
		return errors.New("Unable to find file to upload")
	}

	if sc.Params[uploadURLParamsKey] == "" {
		return upstorage.ErrUnableUpload
	}

	reader, err := file.ReadContent()

	if err != nil {
		return errors.Wrap(err, "Reading file content")
	}

	defer reader.Close()

	buf := bytes.Buffer{}
	_, err = buf.ReadFrom(reader)

	if err != nil {
		return errors.Wrap(err, "Failed to buffer reader")
	}

	req, err := http.NewRequestWithContext(context.TODO(), "PUT", sc.Params[uploadURLParamsKey], &buf)

	if err != nil {
		return errors.Wrap(err, "Building request")
	}

	req.Header.Set("Content-Type", DefaultContentType)
	defer req.Body.Close()

	httpClient := &http.Client{}
	res, err := httpClient.Do(req)

	if err != nil {
		return errors.Wrap(err, "Performing upload request")
	}

	defer reader.Close()

	if res.StatusCode >= 400 {
		return errors.Wrapf(wrapHTTPError(res), "Failed upload request (url: `%s`)", sc.Params[uploadURLParamsKey])
	}

	return nil
}
