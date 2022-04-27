package upstorage

import "github.com/pkg/errors"

var ErrNotFound = errors.New("Not found")
var ErrUnknown = errors.New("Unknown error")
var ErrUnknownStorageClaimType = errors.New("Unknown storage claim type")
var ErrUnableDownload = errors.New("Unable to download storage claim content")
var ErrUnableUpload = errors.New("Unable to upload storage claim content")
