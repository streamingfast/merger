package merger

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"path"
	"testing"
	"time"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/dbin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBundleReader_ReadSimpleFiles(t *testing.T) {
	bundle := NewTestBundle()

	r := NewBundleReader(context.Background(), bundle, nil)
	r1 := make([]byte, 4)

	read, err := r.Read(r1)
	require.NoError(t, err)
	assert.Equal(t, read, 2)
	assert.Equal(t, r1, []byte{0x1, 0x2, 0x0, 0x0})

	read, err = r.Read(r1)
	require.NoError(t, err)
	assert.Equal(t, read, 2)
	assert.Equal(t, r1, []byte{0x3, 0x4, 0x0, 0x0})

	read, err = r.Read(r1)
	require.NoError(t, err)
	assert.Equal(t, read, 2)
	assert.Equal(t, r1, []byte{0x5, 0x6, 0x0, 0x0})

	read, err = r.Read(r1)
	assert.Equal(t, 0, read)
	assert.Equal(t, err, io.EOF)
}

func TestBundleReader_ReadByChunk(t *testing.T) {
	bundle := NewTestBundle()

	r := NewBundleReader(context.Background(), bundle, nil)
	r1 := make([]byte, 1)

	read, err := r.Read(r1)
	require.NoError(t, err)
	assert.Equal(t, read, 1)
	assert.Equal(t, r1, []byte{0x1})

	read, err = r.Read(r1)
	require.NoError(t, err)
	assert.Equal(t, read, 1)
	assert.Equal(t, r1, []byte{0x2})

	read, err = r.Read(r1)
	require.NoError(t, err)
	assert.Equal(t, read, 1)
	assert.Equal(t, r1, []byte{0x3})
}

func NewTestOneBlockFileFromFile(t *testing.T, fileName string) *OneBlockFile {
	t.Helper()
	data, err := ioutil.ReadFile(path.Join("test_data", fileName))
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)
	return &OneBlockFile{
		canonicalName: fileName,
		filenames:     map[string]struct{}{fileName: Empty},
		blockTime:     time.Now(),
		id:            "",
		num:           0,
		previousID:    "",
		data:          data,
	}
}

func NewTestBundle() []*OneBlockFile {
	bstream.GetBlockWriterHeaderLen = 0

	bt := time.Time{}
	o1 := &OneBlockFile{
		canonicalName: "o1",
		blockTime:     bt,
		data:          []byte{0x1, 0x2},
	}
	o2 := &OneBlockFile{
		canonicalName: "o2",
		blockTime:     bt.Local().Add(1 * time.Second),
		data:          []byte{0x3, 0x4},
	}
	o3 := &OneBlockFile{
		canonicalName: "o3",
		blockTime:     bt.Local().Add(2 * time.Second),
		data:          []byte{0x5, 0x6},
	}
	return []*OneBlockFile{o1, o2, o3}
}

func TestBundleReader_Read_Then_Read_Block(t *testing.T) {
	//important
	bstream.GetBlockWriterHeaderLen = 10

	bundle := []*OneBlockFile{
		NewTestOneBlockFileFromFile(t, "0000000001-20150730T152628.0-13406cb6-b1cb8fa3.dbin"),
		NewTestOneBlockFileFromFile(t, "0000000002-20150730T152657.0-044698c9-13406cb6.dbin"),
		NewTestOneBlockFileFromFile(t, "0000000003-20150730T152728.0-a88cf741-044698c9.dbin"),
	}
	fmt.Println(bundle)

	r := NewBundleReader(context.Background(), bundle, nil)
	allBlockData, err := ioutil.ReadAll(r)
	require.NoError(t, err)
	dbinReader := dbin.NewReader(bytes.NewReader(allBlockData))

	//Reader header once
	_, _, err = dbinReader.ReadHeader()

	//Block 1
	require.NoError(t, err)
	b1, err := dbinReader.ReadMessage()
	require.NoError(t, err)
	require.Equal(t, b1, bundle[0].data[14:])

	//Block 2
	require.NoError(t, err)
	b2, err := dbinReader.ReadMessage()
	require.NoError(t, err)
	require.Equal(t, b2, bundle[1].data[14:])

	//Block 3
	require.NoError(t, err)
	b3, err := dbinReader.ReadMessage()
	require.NoError(t, err)
	require.Equal(t, b3, bundle[2].data[14:])
}
