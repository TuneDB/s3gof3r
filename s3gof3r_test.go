package s3gof3r

import (
	"bytes"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

var b *tB

func init() {

	SetLogger(os.Stderr, "test: ", (log.LstdFlags | log.Lshortfile), true)
	var err error
	b, err = testBucket()
	if err != nil {
		log.Fatal(err)
	}
}

func TestGetReader(t *testing.T) {
	t.Parallel()
	var getTests = []struct {
		path   string
		config *Config
		rSize  int64
		err    error
	}{
		{"t1.test", nil, 1 * kb, nil},
		{"no-md5", &Config{Scheme: "https", Client: ClientWithTimeout(clientTimeout), Md5Check: false}, 1, nil},
		{"NoKey", nil, -1, &RespError{StatusCode: 404, Message: "The specified key does not exist."}},
		{"6_mb_test",
			&Config{Concurrency: 3, PartSize: 1 * mb, NTry: 2, Md5Check: true, Scheme: "https", Client: ClientWithTimeout(3 * time.Second)},
			6 * mb,
			nil},
		{"b1", nil, 1, nil},
		{"0byte", &Config{Scheme: "https", Client: ClientWithTimeout(clientTimeout), Md5Check: false}, 0, nil},
	}

	for _, tt := range getTests {
		if tt.rSize >= 0 {
			err := b.putReader(tt.path, &randSrc{Size: int(tt.rSize)})
			if err != nil {
				t.Fatal(err)
			}
		}

		r, h, err := b.GetReader(tt.path, tt.config)
		if err != nil {
			errComp(tt.err, err, t, tt)
			continue
		}
		t.Logf("headers %v\n", h)
		w := ioutil.Discard

		n, err := io.Copy(w, r)
		if err != nil {
			t.Error(err)
		}
		if n != tt.rSize {
			t.Errorf("Expected size: %d. Actual: %d", tt.rSize, n)

		}
		err = r.Close()
		errComp(tt.err, err, t, tt)

	}
}

func TestPutWriter(t *testing.T) {
	t.Parallel()
	var putTests = []struct {
		path   string
		data   []byte
		header http.Header
		config *Config
		wSize  int64
		err    error
	}{
		{"testfile", []byte("test_data"), nil, nil, 9, nil},
		{"", []byte("test_data"), nil, nil,
			9, &RespError{StatusCode: 400, Message: "A key must be specified"}},
		{"test0byte", []byte(""), nil, nil, 0, nil},
		{"testhg", []byte("foo"), goodHeader(), nil, 3, nil},
		{"testhb", []byte("foo"), badHeader(), nil, 3,
			&RespError{StatusCode: 400, Message: "The encryption method specified is not supported"}},
		{"nomd5", []byte("foo"), goodHeader(),
			&Config{Concurrency: 1, PartSize: 5 * mb, NTry: 1, Md5Check: false, Scheme: "http", Client: http.DefaultClient}, 3, nil},
		{"noconc", []byte("foo"), nil,
			&Config{Concurrency: 0, PartSize: 5 * mb, NTry: 1, Md5Check: true, Scheme: "https", Client: ClientWithTimeout(5 * time.Second)}, 3, nil},
		{"enc test", []byte("test_data"), nil, nil, 9, nil},
	}

	for _, tt := range putTests {
		w, err := b.PutWriter(tt.path, tt.header, tt.config)
		if err != nil {
			errComp(tt.err, err, t, tt)
			continue
		}
		r := bytes.NewReader(tt.data)

		n, err := io.Copy(w, r)
		if err != nil {
			t.Error(err)
		}
		if n != tt.wSize {
			t.Errorf("Expected size: %d. Actual: %d", tt.wSize, n)

		}
		err = w.Close()
		errComp(tt.err, err, t, tt)
	}
}

type putMulti struct {
	path   string
	data   io.Reader
	header http.Header
	config *Config
	wSize  int64
	err    error
}

func TestPutMulti(t *testing.T) {

	t.Parallel()
	var putMultiTests = []putMulti{
		{"5mb_test.test", &randSrc{Size: int(5 * mb)}, goodHeader(), nil, 5 * mb, nil},
		{"10mb_test.test", &randSrc{Size: int(10 * mb)}, goodHeader(),
			&Config{Concurrency: 2, PartSize: 5 * mb, NTry: 2, Md5Check: true, Scheme: "https",
				Client: ClientWithTimeout(2 * time.Second)}, 10 * mb, nil},
		{"11mb_test.test", &randSrc{Size: int(11 * mb)}, goodHeader(),
			&Config{Concurrency: 3, PartSize: 5 * mb, NTry: 2, Md5Check: true, Scheme: "https",
				Client: ClientWithTimeout(5 * time.Second)}, 11 * mb, nil},
		{"timeout.test1", &randSrc{Size: int(5 * mb)}, goodHeader(),
			&Config{Concurrency: 1, PartSize: 5 * mb, NTry: 1, Md5Check: false, Scheme: "https",
				Client: ClientWithTimeout(1 * time.Millisecond)}, 5 * mb,
			errors.New("timeout")},
		{"timeout.test2", &randSrc{Size: int(10 * mb)}, goodHeader(),
			&Config{Concurrency: 1, PartSize: 5 * mb, NTry: 1, Md5Check: true, Scheme: "https",
				Client: ClientWithTimeout(1 * time.Millisecond)}, 10 * mb,
			errors.New("timeout")},
		{"toosmallpart", &randSrc{Size: int(6 * mb)}, goodHeader(),
			&Config{Concurrency: 4, PartSize: 2 * mb, NTry: 3, Md5Check: false, Scheme: "https",
				Client: ClientWithTimeout(5 * time.Second)}, 6 * mb, nil},
	}
	wg := sync.WaitGroup{}
	for _, tt := range putMultiTests {
		w, err := b.PutWriter(tt.path, tt.header, tt.config)
		if err != nil {
			errComp(tt.err, err, t, tt)
			continue
		}
		wg.Add(1)

		go func(w io.WriteCloser, tt putMulti) {
			n, err := io.Copy(w, tt.data)
			if err != nil {
				t.Error(err)
			}
			if n != tt.wSize {
				t.Errorf("Expected size: %d. Actual: %d", tt.wSize, n)

			}
			err = w.Close()
			errComp(tt.err, err, t, tt)
			wg.Done()
		}(w, tt)
	}
	wg.Wait()
}

type tB struct {
	*Bucket
}

func testBucket() (*tB, error) {
	k, err := InstanceKeys()
	if err != nil {
		k, err = EnvKeys()
		if err != nil {
			return nil, err
		}
	}
	bucket := os.Getenv("TEST_BUCKET")
	if bucket == "" {
		return nil, errors.New("TEST_BUCKET must be set in environment")
	}
	s3 := New("", k)
	b := tB{s3.Bucket(bucket)}

	return &b, err
}

func (b *tB) putReader(path string, r io.Reader) error {

	if r == nil {
		return nil // special handling for nil case
	}

	w, err := b.PutWriter(path, nil, nil)
	if err != nil {
		return err
	}
	_, err = io.Copy(w, r)
	if err != nil {
		return err
	}
	err = w.Close()
	if err != nil {
		return err
	}

	return nil
}

func errComp(expect, actual error, t *testing.T, tt interface{}) bool {

	if expect == nil && actual == nil {
		return true
	}

	if expect == nil || actual == nil {
		t.Errorf("called with %v\n Expected: %v\n Actual:   %v\n", tt, expect, actual)
		return false
	}
	if !strings.Contains(actual.Error(), expect.Error()) {
		t.Errorf("called with %v\n Expected: %v\n Actual:   %v\n", tt, expect, actual)
		return false
	}
	return true

}

func goodHeader() http.Header {
	header := make(http.Header)
	header.Add("x-amz-server-side-encryption", "AES256")
	header.Add("x-amz-meta-foometadata", "testmeta")
	return header
}

func badHeader() http.Header {
	header := make(http.Header)
	header.Add("x-amz-server-side-encryption", "AES512")
	return header
}

type randSrc struct {
	Size  int
	total int
}

func (r *randSrc) Read(p []byte) (int, error) {

	n, err := rand.Read(p)
	r.total = r.total + n
	if r.total >= r.Size {
		return n - (r.total - r.Size), io.EOF
	}
	return n, err
}

func ExampleBucket_PutWriter() error {

	k, err := EnvKeys() // get S3 keys from environment
	if err != nil {
		return err
	}
	// Open bucket to put file into
	s3 := New("", k)
	b := s3.Bucket("bucketName")

	// open file to upload
	file, err := os.Open("fileName")
	if err != nil {
		return err
	}

	// Open a PutWriter for upload
	w, err := b.PutWriter(file.Name(), nil, nil)
	if err != nil {
		return err
	}
	if _, err = io.Copy(w, file); err != nil { // Copy into S3
		return err
	}
	if err = w.Close(); err != nil {
		return err
	}
	return nil
}

func ExampleBucket_GetReader() error {

	k, err := EnvKeys() // get S3 keys from environment
	if err != nil {
		return err
	}

	// Open bucket to put file into
	s3 := New("", k)
	b := s3.Bucket("bucketName")

	r, h, err := b.GetReader("keyName", nil)
	if err != nil {
		return err
	}
	// stream to standard output
	if _, err = io.Copy(os.Stdout, r); err != nil {
		return err
	}
	err = r.Close()
	if err != nil {
		return err
	}
	fmt.Println(h) // print key header data
	return nil
}

func TestDelete(t *testing.T) {
	t.Parallel()

	var deleteTests = []struct {
		path  string
		exist bool
		err   error
	}{
		{"delete1", true, nil},
		{"delete 2", false, nil},
		{"/delete 2", false, nil},
	}

	for _, tt := range deleteTests {
		if tt.exist {
			err := b.putReader(tt.path, &randSrc{Size: int(1 * kb)})

			if err != nil {
				t.Fatal(err)
			}
		}
		err := b.Delete(tt.path)
		t.Log(err)
		errComp(tt.err, err, t, tt)
	}

}

func TestGetVersion(t *testing.T) {
	t.Parallel()

	var versionTests = []struct {
		path string
		err  error
	}{
		{"key1", nil},
	}
	for _, tt := range versionTests {
		err := b.putReader(tt.path, &randSrc{Size: int(1 * kb)})
		if err != nil {
			t.Fatal(err)
		}
		// get version id
		r, h, err := b.GetReader(tt.path, nil)
		if err != nil {
			t.Fatal(err)
		}
		r.Close()
		v := h.Get("x-amz-version-id")
		if v == "" {
			t.Logf("versioning not enabled on %s\n", b.Name)
			t.SkipNow()
		}
		// upload again for > 1 version
		err = b.putReader(tt.path, &randSrc{Size: int(1 * kb)})
		if err != nil {
			t.Fatal(err)
		}

		// request first uploaded version
		t.Logf("version id: %s", v)
		p := fmt.Sprintf("%s?versionId=%s", tt.path, v)
		r, _, err = b.GetReader(p, nil)
		if err != nil {
			t.Fatal(err)
		}
		r.Close()
		errComp(tt.err, err, t, tt)
	}

}

func TestPutWriteAfterClose(t *testing.T) {
	t.Parallel()

	w, err := b.PutWriter("test", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	err = w.Close()
	if err != nil {
		t.Fatal(err)
	}

	p := make([]byte, 10)

	_, err = w.Write(p)
	if err != syscall.EINVAL {
		t.Errorf("expected %v on write after close, got %v", syscall.EINVAL, err)
	}

}

func TestGetReadAfterClose(t *testing.T) {
	t.Parallel()

	r, _, err := b.GetReader("test", nil)
	if err != nil {
		t.Fatal(err)
	}
	err = r.Close()
	if err != nil {
		t.Fatal(err)
	}

	_, err = r.Read([]byte("foo"))
	if err != syscall.EINVAL {
		t.Errorf("expected %v on read after close, got %v", syscall.EINVAL, err)
	}

}

func TestPutterAfterError(t *testing.T) {
	t.Parallel()

	w, err := b.PutWriter("test", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	p, ok := w.(*putter)
	if !ok {
		t.Fatal("putter type cast failed")
	}
	terr := fmt.Errorf("test error")
	p.err = terr
	_, err = w.Write([]byte("foo"))
	if err != terr {
		t.Errorf("expected error %v on Write, got %v", terr, err)
	}
	err = w.Close()
	if err != terr {
		t.Errorf("expected error %v on Close, got %v", terr, err)
	}

}

func TestGetterAfterError(t *testing.T) {
	t.Parallel()

	r, _, err := b.GetReader("test", nil)
	if err != nil {
		t.Fatal(err)
	}
	g, ok := r.(*getter)
	if !ok {
		t.Fatal("getter type cast failed")
	}
	terr := fmt.Errorf("test error")
	g.err = terr
	_, err = r.Read([]byte("foo"))
	if err != terr {
		t.Errorf("expected error %v on Read, got %v", terr, err)
	}
	err = r.Close()
	if err != terr {
		t.Errorf("expected error %v on Close, got %v", terr, err)
	}
}

/* Troubleshooting TUNEBI-64, we noticed multiple files being listed in a single SQS message,
but the duplicated files were only loaded once.  One bug masking another.  This test verifies
that GetMultiple isn't responsible for that.  It relies on files currently in the 'tunedb-tetris-beta' S3
bucket.  To run it you'll need to set your tunedb AWS creds in the usual env vars.  */
func TestGetMultiple(t *testing.T) {

	keys, err := EnvKeys()
	if err != nil {
		t.Fatal(err)
		return
	}
	s3 := New("", keys)

	var s3gof3rConfig = DefaultConfig
	s3gof3rConfig.Md5Check = false
	s3gof3rConfig.Scheme = "http"
	s3gof3rConfig.Concurrency = 10
	s3gof3rConfig.NTry = 5
	s3gof3rConfig.Client = ClientWithTimeout(5 * time.Second)

	s3Bucket := s3.Bucket("tunedb-tetris-beta")

	prefix := "/log/retl/7336/log_conversions/1_2/2015/11/"
	importS3Files := []string{
		prefix + "7336-log_conversions-1_2-20151101-8bd224456aa74852bb50f26a6028dd79.csv", // a 5KB file
		prefix + "7336-log_conversions-1_2-20151101-176ae58c9476428592094508a0281c3b.csv", // a 16KB file
		prefix + "7336-log_conversions-1_2-20151101-8bd224456aa74852bb50f26a6028dd79.csv", // dup of the 5KB file
	}

	// a log_id from the 5KB file
	logID := "a9c8d17bf8c51aaa82-20151101-7336"

	getter, err := s3Bucket.GetMultiple(s3gof3rConfig, importS3Files)

	if err != nil {
		t.Fatal("got an error calling GetMultiple():", err)
		return
	}
	defer getter.Close()

	buffer := make([]byte, 30000)
	_, err = getter.Read(buffer)
	if err != nil && err != io.EOF {
		t.Fatal("unexpected error copying getter -> buffer:", err)
		return
	}

	// make sure there are 2 instance of the log_id above
	logIDCount := strings.Count(string(buffer), logID)
	if logIDCount != 2 {
		t.Errorf("Error counting instances of log_id, expected 2 got %d", logIDCount)
		t.Fail()
		return
	}

}
