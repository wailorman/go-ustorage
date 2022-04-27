package local

import (
	"context"
	"io"

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

	if err := file.Create(); err != nil {
		return errors.Wrap(err, "Creating & truncating file for storage claim downloading")
	}

	writer, err := file.WriteContent()

	if err != nil {
		return errors.Wrap(err, "Opening file to download")
	}

	scFile := filh.NewFile(sc.Params[pathParamsKey])

	if !scFile.IsExist() {
		return upstorage.ErrNotFound
	}

	reader, err := scFile.ReadContent()

	if err != nil {
		return errors.Wrap(err, "Opening storage claim file to read")
	}

	_, err = io.Copy(writer, reader)

	if err != nil {
		return errors.Wrap(err, "Copying (downloading) file from storage claim to local copy")
	}

	return nil
}

func (client *StorageClient) Upload(ctx context.Context, sc *upstorage.StorageClaim, file filh.Filer, p chan upstorage.Progresser) error {
	// TODO: notify progress
	// TODO: handle ctx cancel

	if !file.IsExist() {
		return errors.New("Unable to find file to upload")
	}

	scFile := filh.NewFile(sc.Params[pathParamsKey])

	if !scFile.IsExist() {
		return upstorage.ErrNotFound
	}

	reader, err := file.ReadContent()

	if err != nil {
		return errors.Wrap(err, "Reading uploading file")
	}

	writer, err := scFile.WriteContent()

	if err != nil {
		return errors.Wrap(err, "Opening storage claim file to write")
	}

	_, err = io.Copy(writer, reader)

	if err != nil {
		return errors.Wrap(err, "Uploading file")
	}

	return nil
}
