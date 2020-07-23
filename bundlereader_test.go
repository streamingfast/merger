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

	"github.com/dfuse-io/bstream"

	"github.com/dfuse-io/dbin"

	"github.com/stretchr/testify/require"
)

func NewTestBundle() *Bundle {
	bt := time.Time{}
	o1 := &OneBlockFile{
		name:      "o1",
		blockTime: bt,
		data:      []byte{0x1, 0x2},
	}
	o2 := &OneBlockFile{
		name:      "o2",
		blockTime: bt.Local().Add(1 * time.Second),
		data:      []byte{0x3, 0x4},
	}
	o3 := &OneBlockFile{
		name:      "o3",
		blockTime: bt.Local().Add(2 * time.Second),
		data:      []byte{0x5, 0x6},
	}
	return &Bundle{
		fileList: map[string]*OneBlockFile{
			o1.name: o1,
			o2.name: o2,
			o3.name: o3,
		},
	}
}

func TestBundleReader_Read(t *testing.T) {
	bundle := NewTestBundle()
	testCases := []struct {
		name         string
		bundle       *Bundle
		readLength   int
		expectedData []byte
	}{
		{
			name:         "full",
			bundle:       bundle,
			readLength:   6,
			expectedData: []byte{0x1, 0x2, 0x3, 0x4, 0x5, 0x6},
		},
		{
			name:         "full and more",
			bundle:       bundle,
			readLength:   10,
			expectedData: []byte{0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x0, 0x0, 0x0, 0x0},
		},
		{
			name:         "2 files",
			bundle:       bundle,
			readLength:   4,
			expectedData: []byte{0x1, 0x2, 0x3, 0x4},
		},
	}

	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			r := NewBundleReader(context.Background(), c.bundle, nil)
			buffer := make([]byte, c.readLength)
			_, err := r.Read(buffer)
			require.NoError(t, err)
			require.Equal(t, c.expectedData, buffer)
		})
	}
}

func TestBundleReader_Read_EOF(t *testing.T) {
	bundle := NewTestBundle()
	r := NewBundleReader(context.Background(), bundle, nil)
	buffer := make([]byte, 6)
	_, err := r.Read(buffer)
	require.NoError(t, err)
	_, err = r.Read(buffer)
	require.Equal(t, io.EOF, err)
}

func NewTestOneBlockFileFromFile(t *testing.T, fileName string) *OneBlockFile {
	t.Helper()
	data, err := ioutil.ReadFile(path.Join("test_data", fileName))
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)
	return &OneBlockFile{
		name:       fileName,
		blockTime:  time.Now(),
		id:         "",
		num:        0,
		previousID: "",
		data:       data,
	}
}

func TestBundleReader_Read_Then_Read_Block(t *testing.T) {
	//important
	bstream.GetBlockWriterHeaderLen = 10

	bundle := &Bundle{
		fileList: map[string]*OneBlockFile{
			"0000000001-20150730T152628.0-13406cb6-b1cb8fa3.dbin": NewTestOneBlockFileFromFile(t, "0000000001-20150730T152628.0-13406cb6-b1cb8fa3.dbin"),
			"0000000002-20150730T152657.0-044698c9-13406cb6.dbin": NewTestOneBlockFileFromFile(t, "0000000002-20150730T152657.0-044698c9-13406cb6.dbin"),
			"0000000003-20150730T152728.0-a88cf741-044698c9.dbin": NewTestOneBlockFileFromFile(t, "0000000003-20150730T152728.0-a88cf741-044698c9.dbin"),
		},
		lowerBlock:     0,
		chunkSize:      0,
		upperBlockID:   "",
		upperBlockTime: time.Time{},
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
	require.Equal(t, b1, bundle.fileList["0000000001-20150730T152628.0-13406cb6-b1cb8fa3.dbin"].data[14:])

	//Block 2
	require.NoError(t, err)
	b2, err := dbinReader.ReadMessage()
	require.NoError(t, err)
	require.Equal(t, b2, bundle.fileList["0000000002-20150730T152657.0-044698c9-13406cb6.dbin"].data[14:])

	//Block 3
	require.NoError(t, err)
	b3, err := dbinReader.ReadMessage()
	require.NoError(t, err)
	require.Equal(t, b3, bundle.fileList["0000000003-20150730T152728.0-a88cf741-044698c9.dbin"].data[14:])

}