package upstorage

import (
	"context"

	"github.com/pkg/errors"
	"github.com/wailorman/go-filh"
)

var ErrUnknownClaimType = errors.New("UNKNOWN_CLAIM_TYPE")

type ClientsMap map[string]StorageClient

type UniversalClient struct {
	clientsMap ClientsMap
}

func NewUniversalClient(clientsMap ClientsMap) *UniversalClient {
	return &UniversalClient{clientsMap: clientsMap}
}

func (uc *UniversalClient) Download(ctx context.Context, sc *StorageClaim, file filh.Filer, p chan Progresser) error {
	if uc.clientsMap[sc.Kind] == nil {
		return ErrUnknownStorageClaimType
	}

	return uc.clientsMap[sc.Kind].Download(ctx, sc, file, p)
}

func (uc *UniversalClient) Upload(ctx context.Context, sc *StorageClaim, file filh.Filer, p chan Progresser) error {
	if uc.clientsMap[sc.Kind] == nil {
		return ErrUnknownStorageClaimType
	}

	return uc.clientsMap[sc.Kind].Upload(ctx, sc, file, p)
}
