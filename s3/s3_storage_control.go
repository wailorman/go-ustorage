package s3

import (
	"context"
	"net/url"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/wailorman/go-upstorage"
)

var SignedURLDuration = time.Duration(24 * time.Hour)

type StorageControl struct {
	s3Client *s3.S3
	bucket   string
}

type StorageControlOptions struct {
	Host      string
	AccessKey string
	SecretKey string
	Bucket    string
}

func NewStorageControl(opts *StorageControlOptions) (*StorageControl, error) {
	hostUrl, err := url.Parse(opts.Host)

	if err != nil {
		return nil, errors.Wrap(err, "Parsing host url")
	}

	s3Config := &aws.Config{
		Credentials:      credentials.NewStaticCredentials(opts.AccessKey, opts.SecretKey, ""),
		Endpoint:         aws.String(opts.Host),
		Region:           aws.String("us-east-1"),
		S3ForcePathStyle: aws.Bool(true),
		DisableSSL:       aws.Bool(hostUrl.Scheme != "https"),
	}

	newSession, err := session.NewSession(s3Config)

	if err != nil {
		return nil, errors.Wrap(err, "Building session")
	}

	s3Client := s3.New(newSession)

	return &StorageControl{
		s3Client: s3Client,
		bucket:   opts.Bucket,
	}, nil
}

func (sc *StorageControl) AllocateStorageClaim(ctx context.Context, req upstorage.StorageClaimRequest) (*upstorage.StorageClaim, error) {
	prefix := uuid.New().String()
	path := prefix + "/" + req.Suffix

	return sc.GetStorageClaim(ctx, path)
}

func (sc *StorageControl) GetStorageClaim(ctx context.Context, identity string) (*upstorage.StorageClaim, error) {
	uploadURL, downloadURL, err := sc.getURLsForStorageClaim(identity)

	if err != nil {
		return nil, errors.Wrap(err, "Building URL for storage claim")
	}

	claim := &upstorage.StorageClaim{
		Identity: identity,
		Kind:     StorageClaimKind,
		Params: map[string]string{
			uploadURLParamsKey:   uploadURL,
			downloadURLParamsKey: downloadURL,
		},
	}

	return claim, nil
}

func (sc *StorageControl) PurgeStorageClaim(ctx context.Context, identity string) error {
	_, err := sc.s3Client.DeleteObjectWithContext(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(sc.bucket),
		Key:    aws.String(identity),
	})

	if err != nil {
		return castAmzErr(err)
	}

	return nil
}

func (sc *StorageControl) GetStorageClaimDownloadURL(ctx context.Context, identity string) (string, error) {
	_, downloadURL, err := sc.getURLsForStorageClaim(identity)

	if err != nil {
		return "", err
	}

	return downloadURL, nil
}

func (sc *StorageControl) getURLsForStorageClaim(identity string) (uploadUrl string, downloadUrl string, err error) {
	downloadReq, _ := sc.s3Client.GetObjectRequest(&s3.GetObjectInput{
		Bucket: aws.String(sc.bucket),
		Key:    aws.String(identity),
	})

	downloadUrlStr, err := downloadReq.Presign(SignedURLDuration)

	if err != nil {
		return "", "", errors.Wrap(castAmzErr(err), "Signing download url")
	}

	uploadReq, _ := sc.s3Client.PutObjectRequest(&s3.PutObjectInput{
		Bucket: aws.String(sc.bucket),
		Key:    aws.String(identity),
	})

	uploadUrlStr, err := uploadReq.Presign(SignedURLDuration)

	if err != nil {
		return "", "", errors.Wrap(castAmzErr(err), "Signing upload url")
	}

	return uploadUrlStr, downloadUrlStr, nil
}

func castAmzErr(err error) error {
	if err != nil {
		if aErr, ok := err.(awserr.Error); ok {
			if aErr.Code() == s3.ErrCodeNoSuchKey {
				return errors.Wrapf(upstorage.ErrNotFound, "No such S3 key (`%s`)", aErr.Error())
			}
		}
	}

	return err
}
