package upstorage_test

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wailorman/go-filh"
	"github.com/wailorman/go-upstorage"
	"github.com/wailorman/go-upstorage/local"
	"github.com/wailorman/go-upstorage/s3"

	_ "github.com/joho/godotenv/autoload"
)

type testTable struct {
	provider   string
	client     upstorage.StorageClient
	controller upstorage.StorageController
}

// TODO: not found for upload & download

func Test__Full(t *testing.T) {
	tests := []testTable{
		{
			provider:   s3.StorageClaimKind,
			client:     buildS3StorageClient(),
			controller: buildS3StorageControl(),
		},
		{
			provider:   local.StorageClaimKind,
			client:     buildLocalStorageClient(),
			controller: buildLocalStorageControl(),
		},
	}

	for _, test := range tests {
		t.Run(test.provider, func(t *testing.T) {
			allocatedClaim, err := test.controller.AllocateStorageClaim(context.Background(), upstorage.StorageClaimRequest{Suffix: "test"})

			if !assert.NoError(t, err, "Allocating storage claim") {
				return
			}

			uploadingFile := filh.NewFile("test/test_file.txt")

			originalContent, err := uploadingFile.ReadAllContent()

			if !assert.NoError(t, err, "Failed to read original content") {
				return
			}

			err = test.client.Upload(context.Background(), allocatedClaim, uploadingFile, nil)

			if !assert.NoError(t, err, "Uploading file") {
				return
			}

			downloadClaim, err := test.controller.GetStorageClaim(context.Background(), allocatedClaim.Identity)

			if !assert.NoError(t, err, "Failed to get download claim") {
				return
			}

			tempFile := filh.NewFile("tmp/" + test.provider + "/client/test_file.txt")

			err = tempFile.EnsureParentDirExists()

			if !assert.NoError(t, err, "Creating download file parent dir") {
				return
			}

			err = test.client.Download(context.Background(), downloadClaim, tempFile, nil)

			if !assert.NoError(t, err, "Failed to download content") {
				return
			}

			downloadedContent, err := tempFile.ReadAllContent()

			if !assert.NoError(t, err, "Failed to read downloaded content") {
				return
			}

			assert.Equal(t, originalContent, downloadedContent, "Wrong content")
		})
	}
}

func buildS3StorageControl() upstorage.StorageController {
	storageControl, err := s3.NewStorageControl(&s3.StorageControlOptions{
		Host:      os.Getenv("S3_HOST"),
		AccessKey: os.Getenv("S3_ACCESS_KEY"),
		SecretKey: os.Getenv("S3_SECRET_KEY"),
		Bucket:    os.Getenv("S3_BUCKET"),
	})

	if err != nil {
		panic(err)
	}

	return storageControl
}

func buildS3StorageClient() upstorage.StorageClient {
	storageClient := s3.NewStorageClient()

	return storageClient
}

func buildLocalStorageControl() upstorage.StorageController {
	storagePath := filh.NewPath("tmp/local/storage/")
	err := storagePath.Create()

	if err != nil {
		panic(err)
	}

	storageControl := local.NewStorageControl(&local.StorageControlOptions{Path: storagePath.FullPath()})

	return storageControl
}

func buildLocalStorageClient() upstorage.StorageClient {
	storageClient := local.NewStorageClient()

	return storageClient
}
