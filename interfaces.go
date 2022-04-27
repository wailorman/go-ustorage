package upstorage

import (
	"context"

	"github.com/wailorman/go-filh"
)

type Progresser interface {
	Progress() float64
}

type StorageClaim struct {
	Identity string
	Kind     string
	Params   map[string]string
}

type StorageClaimRequest struct {
	Suffix string
}

type StorageController interface {
	AllocateStorageClaim(ctx context.Context, req StorageClaimRequest) (*StorageClaim, error)
	PurgeStorageClaim(ctx context.Context, identity string) error
	GetStorageClaim(ctx context.Context, identity string) (*StorageClaim, error)
}

type StorageClient interface {
	Download(ctx context.Context, sc *StorageClaim, file filh.Filer, p chan Progresser) error
	Upload(ctx context.Context, sc *StorageClaim, file filh.Filer, p chan Progresser) error
}
