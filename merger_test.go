// Copyright 2019 dfuse Platform Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package merger

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"google.golang.org/grpc/metadata"

	"github.com/dfuse-io/bstream"
	"github.com/streamingfast/dbin"
	"github.com/streamingfast/dstore"
	pbbstream "github.com/streamingfast/pbgo/dfuse/bstream/v1"
	pb "github.com/streamingfast/pbgo/dfuse/merger/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Hopefully, this block kind value will never be used!
var TestProtocol = pbbstream.Protocol(0xFFFFFF)

func init() {
	bstream.GetBlockReaderFactory = bstream.BlockReaderFactoryFunc(func(reader io.Reader) (bstream.BlockReader, error) {
		return &bstream.TestBlockReaderBin{
			DBinReader: dbin.NewReader(reader),
		}, nil
	})

	bstream.GetBlockWriterFactory = bstream.BlockWriterFactoryFunc(func(writer io.Writer) (bstream.BlockWriter, error) {
		return &bstream.TestBlockWriterBin{
			DBinWriter: dbin.NewWriter(writer),
		}, nil
	})
}

func NewTestBlock(id string, num uint64) *bstream.Block {
	return &bstream.Block{
		Id:             id,
		Number:         num,
		PreviousId:     "",
		Timestamp:      time.Time{},
		LibNum:         0,
		PayloadKind:    TestProtocol,
		PayloadVersion: 0,
		PayloadBuffer:  []byte{0x0},
	}
}

func writeOneBlockFile(t *testing.T, block *bstream.Block, filename string, store dstore.Store) {
	t.Helper()
	buffer := bytes.NewBuffer([]byte{})
	blockWriter, err := bstream.GetBlockWriterFactory.New(buffer)
	require.NoError(t, err)

	err = blockWriter.Write(block)
	require.NoError(t, err)

	err = store.WriteObject(
		context.Background(),
		filename,
		bytes.NewReader(buffer.Bytes()),
	)
	require.NoError(t, err)
}

func setupMerger(t *testing.T) (m *Merger, src dstore.Store, dst dstore.Store, cleanup func()) {
	t.Helper()

	srcdir, err := ioutil.TempDir("", "")
	require.NoError(t, err)

	dstdir, err := ioutil.TempDir("", "")
	require.NoError(t, err)

	src, err = dstore.NewDBinStore(srcdir)
	require.NoError(t, err)

	dst, err = dstore.NewDBinStore(dstdir)
	require.NoError(t, err)

	m = NewMerger(
		src,
		dst,
		0,
		5,
		NewSeenBlockCacheInMemory(100, 0),
		100,
		0,
		0,
		"",
		2,
		100,
		false,
	)

	return m, src, dst, func() {
		os.RemoveAll(srcdir)
		os.RemoveAll(dstdir)
	}
}

func TestMergeUpload(t *testing.T) {
	bstream.GetBlockWriterHeaderLen = 0 //because GetBlockWriterFactory return a writer that do not add the header

	m, oneStore, multiStore, cleanup := setupMerger(t)
	fmt.Println("Merger setup completed")
	defer cleanup()

	fmt.Println("about to write one block files")
	writeOneBlockFile(t,
		NewTestBlock("dfe2e70d6c116a541101cecbb256d7402d62125f6ddc9b607d49edc989825c64", 100),
		"0000000100-19700117T153111.4-dfe2e70d6c116a541101cecbb256d7402d62125f6ddc9b607d49edc989825c64-db10afd3efa45327eb284c83cc925bd9bd7966aea53067c1eebe0724d124ec1e",
		oneStore,
	)
	fmt.Println("Written dfe2e70d6c116a541101cecbb256d7402d62125f6ddc9b607d49edc989825c64")

	writeOneBlockFile(t,
		NewTestBlock("4f66fd0241681ebbc119f97e952c1036b87b6e8f64f5c5d84c5c7a9bb1ebfdcc", 101),
		"0000000101-19700117T153112.4-4f66fd0241681ebbc119f97e952c1036b87b6e8f64f5c5d84c5c7a9bb1ebfdcc-dfe2e70d6c116a541101cecbb256d7402d62125f6ddc9b607d49edc989825c64",
		oneStore,
	)

	writeOneBlockFile(t,
		NewTestBlock("16110f3aa1895de2ec22cfd746751f724d112a953c71b62858a1523b50f3dc64", 102),
		"0000000102-19700117T153113.4-16110f3aa1895de2ec22cfd746751f724d112a953c71b62858a1523b50f3dc64-4f66fd0241681ebbc119f97e952c1036b87b6e8f64f5c5d84c5c7a9bb1ebfdcc",
		oneStore,
	)

	writeOneBlockFile(t,
		NewTestBlock("39bef3da2cd14e02781b576050dc426606149bff937a4af43e65417e6e98c713", 103),
		"0000000103-19700117T153114.4-39bef3da2cd14e02781b576050dc426606149bff937a4af43e65417e6e98c713-16110f3aa1895de2ec22cfd746751f724d112a953c71b62858a1523b50f3dc64",
		oneStore,
	)
	writeOneBlockFile(t,
		NewTestBlock("7faae5e905007d146c15b22dcb736935cb344f88be0d35fe656701e84d52398e", 104),
		"0000000104-19700117T153115.4-7faae5e905007d146c15b22dcb736935cb344f88be0d35fe656701e84d52398e-39bef3da2cd14e02781b576050dc426606149bff937a4af43e65417e6e98c713",
		oneStore,
	)

	fmt.Println("one block files created")

	m.triageNewOneBlockFiles([]string{
		"0000000100-19700117T153111.4-dfe2e70d6c116a541101cecbb256d7402d62125f6ddc9b607d49edc989825c64-db10afd3efa45327eb284c83cc925bd9bd7966aea53067c1eebe0724d124ec1e",
		"0000000101-19700117T153112.4-4f66fd0241681ebbc119f97e952c1036b87b6e8f64f5c5d84c5c7a9bb1ebfdcc-dfe2e70d6c116a541101cecbb256d7402d62125f6ddc9b607d49edc989825c64",
		"0000000102-19700117T153113.4-16110f3aa1895de2ec22cfd746751f724d112a953c71b62858a1523b50f3dc64-4f66fd0241681ebbc119f97e952c1036b87b6e8f64f5c5d84c5c7a9bb1ebfdcc",
		"0000000103-19700117T153114.4-39bef3da2cd14e02781b576050dc426606149bff937a4af43e65417e6e98c713-16110f3aa1895de2ec22cfd746751f724d112a953c71b62858a1523b50f3dc64",
		"0000000104-19700117T153115.4-7faae5e905007d146c15b22dcb736935cb344f88be0d35fe656701e84d52398e-39bef3da2cd14e02781b576050dc426606149bff937a4af43e65417e6e98c713",
	})

	fmt.Println("Triage completed")
	m.mergeUpload()
	fmt.Println("Upload files completed")

	readBack, err := multiStore.OpenObject(context.Background(), "0000000100")
	require.NoError(t, err)

	readBackBlocks, err := bstream.GetBlockReaderFactory.New(readBack)
	require.NoError(t, err)

	b1 := mustReadBlock(t, readBackBlocks)
	b2 := mustReadBlock(t, readBackBlocks)
	b3 := mustReadBlock(t, readBackBlocks)
	b4 := mustReadBlock(t, readBackBlocks)
	b5 := mustReadBlock(t, readBackBlocks)

	blockEnd, err := readBackBlocks.Read()
	require.Nil(t, blockEnd)
	require.Equal(t, io.EOF, err)

	assert.Equal(t, "#100 (dfe2e70d6c116a541101cecbb256d7402d62125f6ddc9b607d49edc989825c64)", b1.String())
	assert.Equal(t, "#101 (4f66fd0241681ebbc119f97e952c1036b87b6e8f64f5c5d84c5c7a9bb1ebfdcc)", b2.String())
	assert.Equal(t, "#102 (16110f3aa1895de2ec22cfd746751f724d112a953c71b62858a1523b50f3dc64)", b3.String())
	assert.Equal(t, "#103 (39bef3da2cd14e02781b576050dc426606149bff937a4af43e65417e6e98c713)", b4.String())
	assert.Equal(t, "#104 (7faae5e905007d146c15b22dcb736935cb344f88be0d35fe656701e84d52398e)", b5.String())
}

type testBlockFile struct {
	id       string
	filename string
	num      uint64
}

var blk100 = &testBlockFile{
	id:       "dfe2e70d6c116a541101cecbb256d7402d62125f6ddc9b607d49edc989825c64",
	num:      100,
	filename: "0000000100-19700117T153111.4-dfe2e70d6c116a541101cecbb256d7402d62125f6ddc9b607d49edc989825c64-db10afd3efa45327eb284c83cc925bd9bd7966aea53067c1eebe0724d124ec1e",
}
var blk101 = &testBlockFile{
	id:       "4f66fd0241681ebbc119f97e952c1036b87b6e8f64f5c5d84c5c7a9bb1ebfdcc",
	num:      101,
	filename: "0000000101-19700117T153112.4-4f66fd0241681ebbc119f97e952c1036b87b6e8f64f5c5d84c5c7a9bb1ebfdcc-dfe2e70d6c116a541101cecbb256d7402d62125f6ddc9b607d49edc989825c64",
}
var blk102 = &testBlockFile{
	id:       "16110f3aa1895de2ec22cfd746751f724d112a953c71b62858a1523b50f3dc64",
	num:      102,
	filename: "0000000102-19700117T153113.4-16110f3aa1895de2ec22cfd746751f724d112a953c71b62858a1523b50f3dc64-4f66fd0241681ebbc119f97e952c1036b87b6e8f64f5c5d84c5c7a9bb1ebfdcc",
}
var blk103 = &testBlockFile{
	id:       "39bef3da2cd14e02781b576050dc426606149bff937a4af43e65417e6e98c713",
	num:      103,
	filename: "0000000103-19700117T153114.4-39bef3da2cd14e02781b576050dc426606149bff937a4af43e65417e6e98c713-16110f3aa1895de2ec22cfd746751f724d112a953c71b62858a1523b50f3dc64",
}
var blk104 = &testBlockFile{
	id:       "7faae5e905007d146c15b22dcb736935cb344f88be0d35fe656701e84d52398e",
	num:      104,
	filename: "0000000104-19700117T153115.4-7faae5e905007d146c15b22dcb736935cb344f88be0d35fe656701e84d52398e-39bef3da2cd14e02781b576050dc426606149bff937a4af43e65417e6e98c713",
}

type TestMerger_PreMergedBlocksServer struct {
	responses []*pb.Response
}

func (s *TestMerger_PreMergedBlocksServer) Send(response *pb.Response) error {
	s.responses = append(s.responses, response)
	return nil
}

func (s *TestMerger_PreMergedBlocksServer) SetHeader(md metadata.MD) error {
	return nil
}

func (s *TestMerger_PreMergedBlocksServer) SendHeader(md metadata.MD) error {
	panic("implement me")
}

func (s *TestMerger_PreMergedBlocksServer) SetTrailer(md metadata.MD) {
	panic("implement me")
}

func (s *TestMerger_PreMergedBlocksServer) Context() context.Context {
	return context.Background()
}

func (s *TestMerger_PreMergedBlocksServer) SendMsg(m interface{}) error {
	panic("implement me")
}

func (s *TestMerger_PreMergedBlocksServer) RecvMsg(m interface{}) error {
	panic("implement me")
}

func TestPreMergedBlocks(t *testing.T) {

	tests := []struct {
		name             string
		writeBlocks      []*testBlockFile
		lowBlockNum      uint64
		highBlockID      string
		expectedBlockIDs []string
		expectedFound    bool
	}{
		{
			name:             "perfect",
			writeBlocks:      []*testBlockFile{blk100, blk101, blk102, blk103, blk104},
			lowBlockNum:      100,
			highBlockID:      blk104.id,
			expectedBlockIDs: []string{blk100.id, blk101.id, blk102.id, blk103.id, blk104.id},
			expectedFound:    true,
		},
		{
			name:             "same low NewTestBlock as high",
			writeBlocks:      []*testBlockFile{blk100, blk101, blk102, blk103, blk104},
			lowBlockNum:      100,
			highBlockID:      blk100.id,
			expectedBlockIDs: []string{blk100.id},
			expectedFound:    true,
		},
		{
			name:             "partial low",
			writeBlocks:      []*testBlockFile{blk100, blk101, blk102, blk103, blk104},
			lowBlockNum:      100,
			highBlockID:      blk103.id,
			expectedBlockIDs: []string{blk100.id, blk101.id, blk102.id, blk103.id},
			expectedFound:    true,
		},
		{
			name:             "partial high",
			writeBlocks:      []*testBlockFile{blk100, blk101, blk102, blk103, blk104},
			lowBlockNum:      102,
			highBlockID:      blk104.id,
			expectedBlockIDs: []string{blk102.id, blk103.id, blk104.id},
			expectedFound:    true,
		},
		{
			name:          "high ID not found",
			writeBlocks:   []*testBlockFile{blk100, blk101, blk102, blk103},
			lowBlockNum:   100,
			highBlockID:   blk104.id,
			expectedFound: false,
		},
		{
			name:          "low num too low",
			writeBlocks:   []*testBlockFile{blk100, blk101, blk102, blk103, blk104},
			lowBlockNum:   99,
			highBlockID:   blk104.id,
			expectedFound: false,
		},
		{
			name:          "low num too high",
			writeBlocks:   []*testBlockFile{blk100, blk101, blk102, blk103, blk104},
			lowBlockNum:   200,
			highBlockID:   blk104.id,
			expectedFound: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {

			m, oneStore, _, cleanup := setupMerger(t)
			defer cleanup()

			var writtenFileNames []string
			for _, blk := range test.writeBlocks {
				writeOneBlockFile(t,
					NewTestBlock(blk.id, blk.num),
					blk.filename,
					oneStore,
				)
				writtenFileNames = append(writtenFileNames, blk.filename)
			}

			m.triageNewOneBlockFiles(writtenFileNames)

			server := &TestMerger_PreMergedBlocksServer{}

			err := m.PreMergedBlocks(
				&pb.Request{
					LowBlockNum: test.lowBlockNum,
					HighBlockID: test.highBlockID,
				},
				server,
			)
			if !test.expectedFound {
				require.Error(t, err)
				assert.Equal(t, 0, len(server.responses))
			}

			assert.Len(t, server.responses, len(test.expectedBlockIDs))
			var foundBlockIDs []string
			for _, resp := range server.responses {
				blk := resp.Block
				foundBlockIDs = append(foundBlockIDs, blk.GetId())
			}
			assert.EqualValues(t, test.expectedBlockIDs, foundBlockIDs)
		})
	}
}

func mustReadBlock(t *testing.T, reader bstream.BlockReader) *bstream.Block {
	t.Helper()

	block, err := reader.Read()
	require.NoError(t, err)

	return block
}
