package local

import (
	"context"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/wailorman/go-filh"
	"github.com/wailorman/go-upstorage"
)

type StorageControl struct {
	storagePath filh.Pather
}

type StorageControlOptions struct {
	Path string
}

func NewStorageControl(opts *StorageControlOptions) *StorageControl {
	return &StorageControl{
		storagePath: filh.NewPath(opts.Path),
	}
}

func (sc *StorageControl) AllocateStorageClaim(ctx context.Context, req upstorage.StorageClaimRequest) (*upstorage.StorageClaim, error) {
	prefix := uuid.New().String()
	path := prefix + "/" + req.Suffix
	file := sc.storagePath.BuildFile(path)

	err := file.EnsureParentDirExists()

	if err != nil {
		return nil, errors.Wrap(err, "Creating directory for storage claim")
	}

	err = file.Create()

	if err != nil {
		return nil, errors.Wrap(err, "Creating file for storage claim")
	}

	claim := &upstorage.StorageClaim{
		Kind:     StorageClaimKind,
		Identity: path,
		Params: map[string]string{
			pathParamsKey: file.FullPath(),
		},
	}

	return claim, nil
}

func (sc *StorageControl) GetStorageClaim(ctx context.Context, identity string) (*upstorage.StorageClaim, error) {
	file := sc.storagePath.BuildFile(identity)

	if !file.IsExist() {
		return nil, errors.Wrap(upstorage.ErrNotFound, "Missing local file")
	}

	return &upstorage.StorageClaim{
		Identity: identity,
		Kind:     StorageClaimKind,
		Params: map[string]string{
			pathParamsKey: file.FullPath(),
		},
	}, nil
}

func (sc *StorageControl) PurgeStorageClaim(ctx context.Context, identity string) error {
	file := sc.storagePath.BuildFile(identity)

	if !file.IsExist() {
		return errors.Wrap(upstorage.ErrNotFound, "Missing local file")
	}

	err := file.BuildPath().Destroy()

	if err != nil {
		return errors.Wrap(err, "Removing file")
	}

	return nil
}
