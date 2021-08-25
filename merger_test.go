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
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"testing"
	"time"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/bstream/forkable"
	"github.com/streamingfast/merger/bundle"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewMerger_SunnyPath(t *testing.T) {
	bundler := bundle.NewBundler(5, 5)

	merger := NewMerger(bundler, time.Second, "", nil, nil, nil, nil, nil, "")

	merger.fetchMergedFileFunc = func(lowBlockNum uint64) ([]*bundle.OneBlockFile, error) {
		return nil, fmt.Errorf("nada")
	}

	srcOneBlockFiles := []*bundle.OneBlockFile{
		bundle.MustNewOneBlockFile("0000000001-20210728T105016.01-00000001a-00000000a-0"),
		bundle.MustNewOneBlockFile("0000000002-20210728T105016.02-00000002a-00000001a-0"),
		bundle.MustNewOneBlockFile("0000000003-20210728T105016.03-00000003a-00000002a-0"),
		bundle.MustNewOneBlockFile("0000000004-20210728T105016.06-00000004a-00000003a-2"),
		bundle.MustNewOneBlockFile("0000000006-20210728T105016.08-00000006a-00000004a-2"),
	}

	merger.fetchOneBlockFiles = func(ctx context.Context) (oneBlockFiles []*bundle.OneBlockFile, err error) {
		return srcOneBlockFiles, nil

	}

	var deletedFiles []*bundle.OneBlockFile
	merger.deleteFilesFunc = func(oneBlockFiles []*bundle.OneBlockFile) {
		deletedFiles = append(deletedFiles, oneBlockFiles...)
	}

	var mergedFiles []*bundle.OneBlockFile
	merger.mergeUploadFunc = func(inclusiveLowerBlock uint64, oneBlockFiles []*bundle.OneBlockFile) (err error) {
		defer merger.Shutdown(nil)
		mergedFiles = oneBlockFiles
		return nil
	}

	go func() {
		select {
		case <-time.After(time.Second):
			panic("too long")
		case <-merger.Terminated():
		}
	}()

	err := merger.launch()
	require.NoError(t, err)

	assert.Len(t, deletedFiles, 1)
	assert.Equal(t, srcOneBlockFiles[0:1], deletedFiles)
	assert.Len(t, mergedFiles, 4)
	assert.Equal(t, srcOneBlockFiles[0:4], mergedFiles)

}

func TestNewMerger_Unlinkable_File(t *testing.T) {
	bundler := bundle.NewBundler(5, 5)
	merger := NewMerger(bundler, time.Second, "", nil, nil, nil, nil, nil, "")
	merger.fetchMergedFileFunc = func(lowBlockNum uint64) ([]*bundle.OneBlockFile, error) {
		return nil, fmt.Errorf("nada")
	}

	srcOneBlockFiles := []*bundle.OneBlockFile{
		bundle.MustNewOneBlockFile("0000000001-20210728T105016.01-00000001a-00000000a-0"),
		bundle.MustNewOneBlockFile("0000000002-20210728T105016.02-00000002a-00000001a-0"),
		bundle.MustNewOneBlockFile("0000000003-20210728T105016.03-00000003a-00000002a-0"),
		bundle.MustNewOneBlockFile("0000000004-20210728T105016.06-00000004a-00000003a-3"),
		bundle.MustNewOneBlockFile("0000000006-20210728T105016.08-00000006a-00000004a-4"),
		bundle.MustNewOneBlockFile("0000000002-20210728T105016.09-00000002b-00000001b-0"), //un linkable file
	}

	merger.fetchOneBlockFiles = func(ctx context.Context) (oneBlockFiles []*bundle.OneBlockFile, err error) {
		oneBlockFiles = srcOneBlockFiles
		return
	}

	var deletedFiles []*bundle.OneBlockFile
	merger.deleteFilesFunc = func(oneBlockFiles []*bundle.OneBlockFile) {
		deletedFiles = append(deletedFiles, oneBlockFiles...)
	}

	var mergedFiles []*bundle.OneBlockFile
	merger.mergeUploadFunc = func(inclusiveLowerBlock uint64, oneBlockFiles []*bundle.OneBlockFile) (err error) {
		defer merger.Shutdown(nil)
		mergedFiles = oneBlockFiles
		return nil
	}

	go func() {
		select {
		case <-time.After(time.Second):
			panic("too long")
		case <-merger.Terminated():
		}
	}()

	err := merger.launch()
	require.NoError(t, err)

	expectedDeleted := append(clone(srcOneBlockFiles[0:2]), srcOneBlockFiles[5])
	require.Equal(t, bundle.ToSortedIDs(expectedDeleted), bundle.ToSortedIDs(deletedFiles))

	expectedMerged := append(clone(srcOneBlockFiles[0:4]), srcOneBlockFiles[5])
	require.Equal(t, bundle.ToIDs(expectedMerged), bundle.ToIDs(mergedFiles))
}

func TestNewMerger_File_Too_Old(t *testing.T) {
	bundler := bundle.NewBundler(5, 5)
	merger := NewMerger(bundler, time.Second, "", nil, nil, nil, nil, nil, "")
	merger.fetchMergedFileFunc = func(lowBlockNum uint64) ([]*bundle.OneBlockFile, error) {
		return nil, fmt.Errorf("nada")
	}

	srcOneBlockFiles := [][]*bundle.OneBlockFile{
		{
			bundle.MustNewOneBlockFile("0000000001-20210728T105016.01-00000001a-00000000a-0"),
			bundle.MustNewOneBlockFile("0000000002-20210728T105016.02-00000002a-00000001a-0"),
			bundle.MustNewOneBlockFile("0000000003-20210728T105016.03-00000003a-00000002a-0"),
			bundle.MustNewOneBlockFile("0000000004-20210728T105016.06-00000004a-00000003a-3"),
			bundle.MustNewOneBlockFile("0000000006-20210728T105016.08-00000006a-00000004a-4"),
		},
		{
			bundle.MustNewOneBlockFile("0000000002-20210728T105016.09-00000002b-00000001b-0"), //too old
		},
	}
	fetchOneBlockFilesCallCount := 0
	merger.fetchOneBlockFiles = func(ctx context.Context) (oneBlockFiles []*bundle.OneBlockFile, err error) {
		oneBlockFiles = srcOneBlockFiles[fetchOneBlockFilesCallCount]
		fetchOneBlockFilesCallCount++
		if fetchOneBlockFilesCallCount == 2 {
			defer merger.Shutdown(nil)
		}
		return
	}

	var deletedFiles []*bundle.OneBlockFile
	merger.deleteFilesFunc = func(oneBlockFiles []*bundle.OneBlockFile) {
		deletedFiles = append(deletedFiles, oneBlockFiles...)
	}

	var mergedFiles []*bundle.OneBlockFile
	merger.mergeUploadFunc = func(inclusiveLowerBlock uint64, oneBlockFiles []*bundle.OneBlockFile) (err error) {
		mergedFiles = oneBlockFiles
		return nil
	}

	go func() {
		select {
		case <-time.After(time.Second):
			panic("too long")
		case <-merger.Terminated():
		}
	}()

	err := merger.launch()
	require.NoError(t, err)

	require.Equal(t, 2, fetchOneBlockFilesCallCount)

	expectedDeleted := append(clone(srcOneBlockFiles[0][0:2]), srcOneBlockFiles[1][0]) //normal purge and too old file
	require.Equal(t, bundle.ToSortedIDs(expectedDeleted), bundle.ToSortedIDs(deletedFiles))

	expectedMerged := clone(srcOneBlockFiles[0][0:4])
	require.Equal(t, bundle.ToIDs(expectedMerged), bundle.ToIDs(mergedFiles))
}

func clone(in []*bundle.OneBlockFile) (out []*bundle.OneBlockFile) {
	out = make([]*bundle.OneBlockFile, len(in))
	copy(out, in)
	return
}

func TestNewMerger_Wait_For_Files(t *testing.T) {
	bundler := bundle.NewBundler(5, 5)

	merger := NewMerger(bundler, time.Second, "", nil, nil, nil, nil, nil, "")

	merger.fetchMergedFileFunc = func(lowBlockNum uint64) ([]*bundle.OneBlockFile, error) {
		return nil, fmt.Errorf("nada")
	}

	srcOneBlockFiles := [][]*bundle.OneBlockFile{
		{},
		{
			bundle.MustNewOneBlockFile("0000000001-20210728T105016.01-00000001a-00000000a-0"),
			bundle.MustNewOneBlockFile("0000000002-20210728T105016.02-00000002a-00000001a-0"),
			bundle.MustNewOneBlockFile("0000000003-20210728T105016.03-00000003a-00000002a-0"),
			bundle.MustNewOneBlockFile("0000000004-20210728T105016.06-00000004a-00000003a-2"),
		},
		{
			bundle.MustNewOneBlockFile("0000000006-20210728T105016.08-00000006a-00000004a-2"),
		},
	}

	fetchOneBlockFilesCallCount := 0
	merger.fetchOneBlockFiles = func(ctx context.Context) (oneBlockFiles []*bundle.OneBlockFile, err error) {
		oneBlockFiles = srcOneBlockFiles[fetchOneBlockFilesCallCount]
		fetchOneBlockFilesCallCount++
		return
	}

	var deletedFiles []*bundle.OneBlockFile
	merger.deleteFilesFunc = func(oneBlockFiles []*bundle.OneBlockFile) {
		deletedFiles = append(deletedFiles, oneBlockFiles...)
	}

	var mergedFiles []*bundle.OneBlockFile
	merger.mergeUploadFunc = func(inclusiveLowerBlock uint64, oneBlockFiles []*bundle.OneBlockFile) (err error) {
		defer merger.Shutdown(nil)
		mergedFiles = oneBlockFiles
		return nil
	}

	go func() {
		select {
		case <-time.After(time.Second):
			panic("too long")
		case <-merger.Terminated():
		}
	}()

	err := merger.launch()
	require.NoError(t, err)

	assert.Len(t, deletedFiles, 1)
	assert.Equal(t, srcOneBlockFiles[1][0:1], deletedFiles)
	assert.Len(t, mergedFiles, 4)
	assert.Equal(t, srcOneBlockFiles[1], mergedFiles)
}

func TestNewMerger_Multiple_Merge(t *testing.T) {
	bundler := bundle.NewBundler(5, 5)

	merger := NewMerger(bundler, 0, "", nil, nil, nil, nil, nil, "")

	merger.fetchMergedFileFunc = func(lowBlockNum uint64) ([]*bundle.OneBlockFile, error) {
		return nil, fmt.Errorf("nada")
	}

	srcOneBlockFiles := []*bundle.OneBlockFile{
		bundle.MustNewOneBlockFile("0000000001-20210728T105016.01-00000001a-00000000a-0"),
		bundle.MustNewOneBlockFile("0000000002-20210728T105016.02-00000002a-00000001a-0"),
		bundle.MustNewOneBlockFile("0000000003-20210728T105016.03-00000003a-00000002a-0"),
		bundle.MustNewOneBlockFile("0000000004-20210728T105016.06-00000004a-00000003a-0"),

		bundle.MustNewOneBlockFile("0000000006-20210728T105016.08-00000006a-00000004a-0"),
		bundle.MustNewOneBlockFile("0000000007-20210728T105016.09-00000007a-00000006a-0"),
		bundle.MustNewOneBlockFile("0000000008-20210728T105016.10-00000008a-00000007a-0"),
		bundle.MustNewOneBlockFile("0000000009-20210728T105016.11-00000009a-00000008a-0"),

		bundle.MustNewOneBlockFile("0000000010-20210728T105016.12-00000010a-00000009a-0"),
	}

	merger.fetchOneBlockFiles = func(ctx context.Context) (oneBlockFiles []*bundle.OneBlockFile, err error) {
		return srcOneBlockFiles, nil
	}

	merger.deleteFilesFunc = func(oneBlockFiles []*bundle.OneBlockFile) {
		t.Fatalf("should not be call")
	}

	var mergedFiles []*bundle.OneBlockFile
	mergeUploadFuncCallCount := 0
	merger.mergeUploadFunc = func(inclusiveLowerBlock uint64, oneBlockFiles []*bundle.OneBlockFile) (err error) {
		mergeUploadFuncCallCount++

		if mergeUploadFuncCallCount == 2 {
			defer merger.Shutdown(nil)
		}

		mergedFiles = append(mergedFiles, oneBlockFiles...)
		return nil
	}

	go func() {
		select {
		case <-time.After(time.Second):
			panic("too long")
		case <-merger.Terminated():
		}
	}()

	err := merger.launch()
	require.NoError(t, err)
	require.Equal(t, 2, mergeUploadFuncCallCount)
	require.Equal(t, bundle.ToIDs(srcOneBlockFiles[0:8]), bundle.ToIDs(mergedFiles))
}

func TestNewMerger_SunnyPath_With_MergeFile_Already_Exist(t *testing.T) {
	bundler := bundle.NewBundler(5, 5)

	merger := NewMerger(bundler, 0, "", nil, nil, nil, nil, nil, "")

	mergeFiles := map[uint64][]*bundle.OneBlockFile{
		0: {
			bundle.MustNewMergedOneBlockFile("0000000001-20210728T105016.01-00000001a-00000000a-0"),
			bundle.MustNewMergedOneBlockFile("0000000002-20210728T105016.02-00000002a-00000001a-0"),
			bundle.MustNewMergedOneBlockFile("0000000003-20210728T105016.03-00000003a-00000002a-0"),
			bundle.MustNewMergedOneBlockFile("0000000004-20210728T105016.06-00000004a-00000003a-2"),
		},
	}

	var mergeFilesFetched []uint64
	merger.fetchMergedFileFunc = func(lowBlockNum uint64) ([]*bundle.OneBlockFile, error) {
		mergeFilesFetched = append(mergeFilesFetched, lowBlockNum)
		oneBlockFile, found := mergeFiles[lowBlockNum]
		if !found {
			return nil, fmt.Errorf("nada")
		}
		return oneBlockFile, nil
	}

	merger.fetchOneBlockFiles = func(ctx context.Context) (oneBlockFiles []*bundle.OneBlockFile, err error) {
		defer merger.Shutdown(nil)
		return nil, nil
	}

	merger.deleteFilesFunc = func(oneBlockFiles []*bundle.OneBlockFile) {
		t.Fatalf("should not have been call")
	}

	merger.mergeUploadFunc = func(inclusiveLowerBlock uint64, oneBlockFiles []*bundle.OneBlockFile) (err error) {
		t.Fatalf("should not have been call")
		return nil
	}

	go func() {
		select {
		case <-time.After(time.Second):
			panic("too long")
		case <-merger.Terminated():
		}
	}()

	err := merger.launch()
	require.NoError(t, err)

	require.Equal(t, mergeFilesFetched, []uint64{0})
}

func TestNewMerger_SunnyPath_With_Bootstrap(t *testing.T) {
	bundler := bundle.NewBundler(5, 5)

	merger := NewMerger(bundler, time.Second, "", nil, nil, nil, nil, nil, "")

	mergeFiles := map[uint64][]*bundle.OneBlockFile{
		0: {
			bundle.MustNewMergedOneBlockFile("0000000001-20210728T105016.01-00000001a-00000000a-0"),
			bundle.MustNewMergedOneBlockFile("0000000002-20210728T105016.02-00000002a-00000001a-0"),
			bundle.MustNewMergedOneBlockFile("0000000003-20210728T105016.03-00000003a-00000002a-0"),
			bundle.MustNewMergedOneBlockFile("0000000004-20210728T105016.06-00000004a-00000003a-2"),
		},
	}

	var mergeFilesFetched []uint64
	merger.fetchMergedFileFunc = func(lowBlockNum uint64) ([]*bundle.OneBlockFile, error) {
		mergeFilesFetched = append(mergeFilesFetched, lowBlockNum)
		oneBlockFile, found := mergeFiles[lowBlockNum]
		if !found {
			return nil, fmt.Errorf("nada")
		}
		return oneBlockFile, nil
	}

	merger.fetchOneBlockFiles = func(ctx context.Context) (oneBlockFiles []*bundle.OneBlockFile, err error) {
		defer merger.Shutdown(nil)
		return nil, nil

	}

	merger.deleteFilesFunc = func(oneBlockFiles []*bundle.OneBlockFile) {
		t.Fatalf("should not have been call")
	}

	merger.mergeUploadFunc = func(inclusiveLowerBlock uint64, oneBlockFiles []*bundle.OneBlockFile) (err error) {
		t.Fatalf("should not have been call")
		return nil
	}

	err := bundler.Boostrap(merger.fetchMergedFileFunc)
	require.NoError(t, err)

	err = merger.launch()
	require.NoError(t, err)

	require.Equal(t, mergeFilesFetched, []uint64{0, 0}) //one time from the bootstrap and one time from main loop
}

func TestNewMerger_Check_StateFile(t *testing.T) {
	dir, err := ioutil.TempDir("", "")
	require.NoError(t, err)
	dirPath, err := filepath.Abs(filepath.Dir(dir))
	require.NoError(t, err)
	stateFilePath := path.Join(dirPath, "TestNewMerger_Check_StateFile")
	_ = os.Remove(stateFilePath)
	bundler := bundle.NewBundler(5, 5)

	merger := NewMerger(bundler, time.Second, "", nil, nil, nil, nil, nil, stateFilePath)

	merger.fetchMergedFileFunc = func(lowBlockNum uint64) ([]*bundle.OneBlockFile, error) {
		return nil, nil
	}

	srcOneBlockFiles := []*bundle.OneBlockFile{
		bundle.MustNewOneBlockFile("0000000001-20210728T105016.01-00000001a-00000000a-0"),
		bundle.MustNewOneBlockFile("0000000002-20210728T105016.02-00000002a-00000001a-0"),
		bundle.MustNewOneBlockFile("0000000003-20210728T105016.03-00000003a-00000002a-0"),
		bundle.MustNewOneBlockFile("0000000004-20210728T105016.06-00000004a-00000003a-0"),
		bundle.MustNewOneBlockFile("0000000006-20210728T105016.08-00000006a-00000004a-0"),
	}

	merger.fetchOneBlockFiles = func(ctx context.Context) (oneBlockFiles []*bundle.OneBlockFile, err error) {
		return srcOneBlockFiles, nil

	}

	merger.deleteFilesFunc = func(oneBlockFiles []*bundle.OneBlockFile) {
		t.Fatalf("should not be called")
	}

	var mergedFiles []*bundle.OneBlockFile
	merger.mergeUploadFunc = func(inclusiveLowerBlock uint64, oneBlockFiles []*bundle.OneBlockFile) (err error) {
		defer merger.Shutdown(nil)

		mergedFiles = oneBlockFiles
		return nil
	}

	go func() {
		select {
		case <-time.After(time.Second):
			panic("too long")
		case <-merger.Terminated():
		}
	}()

	err = merger.launch()
	require.NoError(t, err)

	require.Len(t, mergedFiles, 4)
	require.Equal(t, mergedFiles, srcOneBlockFiles[0:4])

	state, err := LoadState(stateFilePath)
	require.NoError(t, err)
	require.Equal(t, uint64(10), state.ExclusiveHighestBlockLimit)
}

func TestBundler_Save_Load(t *testing.T) {
	dir, err := ioutil.TempDir("", "")
	require.NoError(t, err)
	dirPath, err := filepath.Abs(filepath.Dir(dir))
	require.NoError(t, err)
	filePath := path.Join(dirPath, "bundle.test.bak")

	db := forkable.NewForkDB()
	db.AddLink(bstream.NewBlockRef("00000106a", 6), "0000010a", nil)
	state := &State{
		ExclusiveHighestBlockLimit: 991,
	}

	err = SaveState(state, filePath)
	require.NoError(t, err)

	reloaded, err := LoadState(filePath)

	require.NoError(t, err)
	require.Equal(t, state.ExclusiveHighestBlockLimit, reloaded.ExclusiveHighestBlockLimit)
}
