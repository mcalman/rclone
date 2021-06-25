package operations

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fstest"
	"github.com/rclone/rclone/fstest/mockobject"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type interruptReader struct{}

func (r *interruptReader) Read(b []byte) (n int, err error) {
	err = syscall.Kill(syscall.Getpid(), syscall.SIGINT)
	return 0, err
}

// this is a wrapper for a mockobject with a custom Open function
//
// breaks indicate the number of bytes to read before sending an
// interrupt signal
type resumeTestObject struct {
	fs.Object
	breaks []int64
}

// Open opens the file for read. Call Close() on the returned io.ReadCloser
//
// This will signal an interrupt after reading the number of bytes in breaks
func (o *resumeTestObject) Open(ctx context.Context, options ...fs.OpenOption) (io.ReadCloser, error) {
	rc, err := o.Object.Open(ctx, options...)
	if err != nil {
		return nil, err
	}
	if len(o.breaks) > 0 {
		// Pop a breakpoint off
		N := o.breaks[0]
		o.breaks = o.breaks[1:]
		// If 0 then return an error immediately
		if N == 0 {
			return nil, errorTestError
		}
		// Read N bytes then an error
		var ir interruptReader
		r := io.MultiReader(&io.LimitedReader{R: rc, N: N}, &ir)
		// Wrap with Close in a new readCloser
		rc = readCloser{Reader: r, Closer: rc}
	}
	return rc, nil
}

func TestResume(t *testing.T) {
	ctx := context.Background()
	r := fstest.NewRun(t)
	defer r.Finalise()
	ci := fs.GetConfig(ctx)
	ci.ResumeLarger = 0

	// Contents for the mock object
	var (
		resumeTestContents = []byte("0123456789")
		expectedContents   = resumeTestContents
	)

	// Create mockobjects with given breaks
	createTestSrc := func(breaks []int64) (fs.Object, fs.Object) {
		srcOrig := mockobject.New("potato").WithContent(resumeTestContents, mockobject.SeekModeNone)
		srcOrig.SetFs(r.Flocal)
		src := &resumeTestObject{
			Object: srcOrig,
			breaks: breaks,
		}
		return src, srcOrig
	}

	checkContents := func(obj fs.Object, contents string) {
		assert.NotNil(t, obj)
		assert.Equal(t, int64(len(contents)), obj.Size())

		r, err := obj.Open(ctx)
		assert.NoError(t, err)
		assert.NotNil(t, r)
		if r == nil {
			return
		}
		data, err := ioutil.ReadAll(r)
		assert.NoError(t, err)
		assert.Equal(t, contents, string(data))
		_ = r.Close()
	}

	srcBreak, srcNoBreak := createTestSrc([]int64{2})

	// Run first Copy only in a subprocess so that it can be interrupted without ending the test
	// adapted from: https://stackoverflow.com/questions/26225513/how-to-test-os-exit-scenarios-in-go
	if os.Getenv("RUNTEST") == "1" {
		remoteRoot := os.Getenv("REMOTEROOT")
		remoteFs, err := fs.NewFs(ctx, remoteRoot)
		require.NoError(t, err)
		_, _ = Copy(ctx, remoteFs, nil, "testdst", srcBreak)
		// This should never be reached as the subroutine should exit during Copy
		require.True(t, false, "Problem with test, first Copy operation should've been interrupted before completion")
		return
	}
	// Start the subprocess
	cmd := exec.Command(os.Args[0], "-test.run=TestResume")
	cmd.Env = append(os.Environ(), "RUNTEST=1", "REMOTEROOT="+r.Fremote.Root())
	cmd.Stdout = os.Stdout
	err := cmd.Run()

	e, ok := err.(*exec.ExitError)

	expectedErrorString := "exit status 1"
	assert.Equal(t, true, ok)
	assert.Equal(t, expectedErrorString, e.Error())

	var buf bytes.Buffer
	log.SetOutput(&buf)
	defer func() {
		log.SetOutput(os.Stderr)
	}()

	// Start copy again, but with no breaks
	newDst, err := Copy(ctx, r.Fremote, nil, "testdst", srcNoBreak)
	assert.NoError(t, err)

	// Checks to see if a resume was initiated
	assert.True(t, strings.Contains(buf.String(), "Resuming at byte position: 2"), "The upload did not resume when restarted.")

	checkContents(newDst, string(expectedContents))
}
