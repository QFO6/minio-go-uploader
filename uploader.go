package uploader

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/revel/revel"
	"github.com/revel/revel/cache"
)

var minioClient = &minio.Client{}

type Config struct {
	Endpoint  string 
	AccessKey string 
	SecretKey string 
	UseSSL    bool  
}

type progressReaderWrapper struct {
	reader        io.Reader
	fileSize      int64
	bytesUploaded int64
	uploadId      string
}

func (pr *progressReaderWrapper) Read(p []byte) (int, error) {
	n, err := pr.reader.Read(p)
	pr.bytesUploaded += int64(n)

	// Calculate and log progress
	progress := float64(pr.bytesUploaded) / float64(pr.fileSize) * 100
	progress = math.Round(progress * 100) / 100
	go cache.Set(pr.uploadId+"progress", progress, time.Hour*2)
	fmt.Println("1 Progress: %.2f%%", progress, pr.uploadId)

	return n, err
}

func PutObject(objectName string, bucketName string, file []byte, fileSize int64, contentType string, uploadId string) error {
	ctx := context.Background()

	// reader := bytes.NewReader(file)
	progressReader := bytes.NewReader(file)
	reader := &progressReaderWrapper{
		reader: progressReader,
		fileSize: progressReader.Size(),
		bytesUploaded: 0,
		uploadId: uploadId,
	}
	
	// Upload the zip file with FPutObject
	uploadInfo, err := minioClient.PutObject(ctx, bucketName, objectName, reader, fileSize, minio.PutObjectOptions{ContentType: contentType})
	if err != nil {
		return err
	}
	fmt.Println("size",uploadInfo.Size)
	return nil
}

func GetList(prefix string, bucketName string) ([]minio.ObjectInfo, error) {
	ret := []minio.ObjectInfo{}
	// Initialize minio client object.
	ctx, cancel := context.WithCancel(context.Background())

	defer cancel()
	
	objectCh := minioClient.ListObjects(ctx, bucketName, minio.ListObjectsOptions{
		Prefix: prefix,
		Recursive: true,
 	})
 for object := range objectCh {
	 if object.Err != nil {
		 fmt.Println(object.Err)
		 return nil,object.Err
	 }
	 ret = append(ret, object)
 }
 return ret, nil
}

func GetObject(objectName string, bucketName string) ([]byte, error) {
	object, err := minioClient.GetObject(context.Background(), bucketName, objectName, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	objectInfo, _ := object.Stat()
	
	defer object.Close()
	b := make([]byte, objectInfo.Size)
	_, err = object.Read(b)
    if err != nil {
    }
	return b, nil
}

func DeleteObject(objectName string, bucketName string) error {

	opts := minio.RemoveObjectOptions{
		GovernanceBypass: true,
	}
	err := minioClient.RemoveObject(context.Background(), bucketName, objectName, opts)
	if err != nil {
		return err
	}
	return nil	
}

func InitMinioClient(config Config) error {
	var err error
	minioClient, err = minio.New(config.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(config.AccessKey, config.SecretKey, ""),
		Secure: config.UseSSL,
	})
	if err != nil {
		revel.AppLog.Errorf(fmt.Sprintf("Fetch vault token failed: %v", err))
		return err
	}
	return nil
}