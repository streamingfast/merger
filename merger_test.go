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
		bundle.MustNewOneBlockFile("0000000001-20210728T105016.01-00000001a-00000000a-0-suffix"),
		bundle.MustNewOneBlockFile("0000000002-20210728T105016.02-00000002a-00000001a-0-suffix"),
		bundle.MustNewOneBlockFile("0000000003-20210728T105016.03-00000003a-00000002a-0-suffix"),
		bundle.MustNewOneBlockFile("0000000004-20210728T105016.06-00000004a-00000003a-2-suffix"),
		bundle.MustNewOneBlockFile("0000000006-20210728T105016.08-00000006a-00000004a-2-suffix"),
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

	assert.Len(t, deletedFiles, 4)
	assert.Equal(t, bundle.ToSortedIDs(srcOneBlockFiles[0:4]), bundle.ToSortedIDs(deletedFiles))
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
		bundle.MustNewOneBlockFile("0000000001-20210728T105016.01-00000001a-00000000a-0-suffix"),
		bundle.MustNewOneBlockFile("0000000002-20210728T105016.02-00000002a-00000001a-0-suffix"),
		bundle.MustNewOneBlockFile("0000000003-20210728T105016.03-00000003a-00000002a-0-suffix"),
		bundle.MustNewOneBlockFile("0000000004-20210728T105016.06-00000004a-00000003a-3-suffix"),
		bundle.MustNewOneBlockFile("0000000006-20210728T105016.08-00000006a-00000004a-4-suffix"),
		bundle.MustNewOneBlockFile("0000000002-20210728T105016.09-00000002b-00000001b-0-suffix"), //un linkable file
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

	expectedDeleted := append(clone(srcOneBlockFiles[0:4]), srcOneBlockFiles[5])
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
			bundle.MustNewOneBlockFile("0000000001-20210728T105016.01-00000001a-00000000a-0-suffix"),
			bundle.MustNewOneBlockFile("0000000002-20210728T105016.02-00000002a-00000001a-0-suffix"),
			bundle.MustNewOneBlockFile("0000000003-20210728T105016.03-00000003a-00000002a-0-suffix"),
			bundle.MustNewOneBlockFile("0000000004-20210728T105016.06-00000004a-00000003a-3-suffix"),
			bundle.MustNewOneBlockFile("0000000006-20210728T105016.08-00000006a-00000004a-4-suffix"),
		},
		{
			bundle.MustNewOneBlockFile("0000000002-20210728T105016.09-00000002b-00000001b-0-suffix"), //too old
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

	expectedDeleted := append(clone(srcOneBlockFiles[0][0:4]), srcOneBlockFiles[1][0]) //normal purge and too old file
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

	merger := NewMerger(bundler, 0, "", nil, nil, nil, nil, nil, "")

	merger.fetchMergedFileFunc = func(lowBlockNum uint64) ([]*bundle.OneBlockFile, error) {
		return nil, fmt.Errorf("nada")
	}

	srcOneBlockFiles := [][]*bundle.OneBlockFile{
		{},
		{
			bundle.MustNewOneBlockFile("0000000001-20210728T105016.01-00000001a-00000000a-0-suffix"),
			bundle.MustNewOneBlockFile("0000000002-20210728T105016.02-00000002a-00000001a-0-suffix"),
			bundle.MustNewOneBlockFile("0000000003-20210728T105016.03-00000003a-00000002a-0-suffix"),
			bundle.MustNewOneBlockFile("0000000004-20210728T105016.06-00000004a-00000003a-2-suffix"),
		},
		{
			bundle.MustNewOneBlockFile("0000000006-20210728T105016.08-00000006a-00000004a-2-suffix"),
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

	assert.Equal(t, bundle.ToSortedIDs(mergedFiles), bundle.ToSortedIDs(deletedFiles))
	assert.Equal(t, srcOneBlockFiles[1], mergedFiles)
}

func TestNewMerger_Multiple_Merge(t *testing.T) {
	bundler := bundle.NewBundler(5, 5)

	merger := NewMerger(bundler, 0, "", nil, nil, nil, nil, nil, "")

	merger.fetchMergedFileFunc = func(lowBlockNum uint64) ([]*bundle.OneBlockFile, error) {
		return nil, fmt.Errorf("nada")
	}

	srcOneBlockFiles := []*bundle.OneBlockFile{
		bundle.MustNewOneBlockFile("0000000001-20210728T105016.01-00000001a-00000000a-0-suffix"),
		bundle.MustNewOneBlockFile("0000000002-20210728T105016.02-00000002a-00000001a-0-suffix"),
		bundle.MustNewOneBlockFile("0000000003-20210728T105016.03-00000003a-00000002a-0-suffix"),
		bundle.MustNewOneBlockFile("0000000004-20210728T105016.06-00000004a-00000003a-0-suffix"),

		bundle.MustNewOneBlockFile("0000000006-20210728T105016.08-00000006a-00000004a-0-suffix"),
		bundle.MustNewOneBlockFile("0000000007-20210728T105016.09-00000007a-00000006a-0-suffix"),
		bundle.MustNewOneBlockFile("0000000008-20210728T105016.10-00000008a-00000007a-0-suffix"),
		bundle.MustNewOneBlockFile("0000000009-20210728T105016.11-00000009a-00000008a-0-suffix"),

		bundle.MustNewOneBlockFile("0000000010-20210728T105016.12-00000010a-00000009a-0-suffix"),
	}

	merger.fetchOneBlockFiles = func(ctx context.Context) (oneBlockFiles []*bundle.OneBlockFile, err error) {
		return srcOneBlockFiles, nil
	}

	var deletedFiles []*bundle.OneBlockFile
	merger.deleteFilesFunc = func(oneBlockFiles []*bundle.OneBlockFile) {
		deletedFiles = append(deletedFiles, oneBlockFiles...)
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

	expectedDeleted := mergedFiles
	require.Equal(t, bundle.ToSortedIDs(expectedDeleted), bundle.ToSortedIDs(deletedFiles))

	require.Equal(t, 2, mergeUploadFuncCallCount)
	require.Equal(t, bundle.ToIDs(srcOneBlockFiles[0:8]), bundle.ToIDs(mergedFiles))
}

func TestNewMerger_SunnyPath_With_MergeFile_Already_Exist(t *testing.T) {
	bundler := bundle.NewBundler(5, 105)

	merger := NewMerger(bundler, 0, "", nil, nil, nil, nil, nil, "")

	mergeFiles := map[uint64][]*bundle.OneBlockFile{
		100: {
			bundle.MustNewOneBlockFile("0000000100-20210728T105016.08-00000100a-00000099a-99-suffix"),
			bundle.MustNewOneBlockFile("0000000101-20210728T105016.09-00000101a-00000100a-99-suffix"),
			bundle.MustNewOneBlockFile("0000000102-20210728T105016.10-00000102a-00000101a-99-suffix"),
			bundle.MustNewOneBlockFile("0000000103-20210728T105016.11-00000103a-00000102a-99-suffix"),
			bundle.MustNewOneBlockFile("0000000104-20210728T105016.12-00000104a-00000103a-99-suffix"),
		},
		105: {
			bundle.MustNewOneBlockFile("0000000105-20210728T105016.13-00000105a-00000104a-100-suffix"),
			bundle.MustNewOneBlockFile("0000000106-20210728T105016.14-00000106a-00000105a-101-suffix"),
			bundle.MustNewOneBlockFile("0000000107-20210728T105016.15-00000107a-00000106a-102-suffix"),
			bundle.MustNewOneBlockFile("0000000108-20210728T105016.16-00000108a-00000107a-103-suffix"),
			bundle.MustNewOneBlockFile("0000000109-20210728T105016.17-00000109a-00000108a-104-suffix"),
		},
		110: {
			bundle.MustNewOneBlockFile("0000000110-20210728T105016.18-00000110a-00000109a-105-suffix"),
			bundle.MustNewOneBlockFile("0000000111-20210728T105016.19-00000111a-00000110a-106-suffix"),
			bundle.MustNewOneBlockFile("0000000112-20210728T105016.20-00000112a-00000111a-107-suffix"),
			bundle.MustNewOneBlockFile("0000000113-20210728T105016.21-00000113a-00000112a-108-suffix"),
			bundle.MustNewOneBlockFile("0000000114-20210728T105016.21-00000114a-00000113a-109-suffix"),
		},
	}

	merger.fetchMergedFileFunc = func(lowBlockNum uint64) ([]*bundle.OneBlockFile, error) {
		oneBlockFile, found := mergeFiles[lowBlockNum]
		if !found {
			return nil, fmt.Errorf("nada")
		}
		if lowBlockNum == 110 {
			defer merger.Shutdown(nil)
		}
		return oneBlockFile, nil
	}

	cycle := 0
	merger.fetchOneBlockFiles = func(ctx context.Context) (oneBlockFiles []*bundle.OneBlockFile, err error) {
		// not actually fetching oneBlockFiles, but it is a good place to add assertions in this loop
		switch cycle {
		case 0:
			num, err := merger.bundler.LongestChainFirstBlockNum()
			require.NoError(t, err)
			require.Equal(t, merger.bundler.BundleInclusiveLowerBlock(), uint64(105))
			require.Equal(t, merger.bundler.ExclusiveHighestBlockLimit(), uint64(110))
			require.Equal(t, num, uint64(100))
		case 1:
			num, err := merger.bundler.LongestChainFirstBlockNum()
			require.NoError(t, err)
			require.Equal(t, merger.bundler.BundleInclusiveLowerBlock(), uint64(110))
			require.Equal(t, merger.bundler.ExclusiveHighestBlockLimit(), uint64(115))
			require.Equal(t, num, uint64(104))
		case 2:
			num, err := merger.bundler.LongestChainFirstBlockNum()
			require.NoError(t, err)
			require.Equal(t, merger.bundler.BundleInclusiveLowerBlock(), uint64(115))
			require.Equal(t, merger.bundler.ExclusiveHighestBlockLimit(), uint64(120))
			require.Equal(t, num, uint64(109))
		default:
			t.Fatalf("Should not happen")
		}
		cycle += 1

		return nil, nil
	}

	merger.deleteFilesFunc = func(oneBlockFiles []*bundle.OneBlockFile) {
		t.Fatalf("Should not happen. Only forkdb should be truncated")
	}

	merger.mergeUploadFunc = func(inclusiveLowerBlock uint64, oneBlockFiles []*bundle.OneBlockFile) (err error) {
		t.Fatalf("should not have been called")
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
}

func TestNewMerger_SunnyPath_With_Bootstrap(t *testing.T) {
	bundler := bundle.NewBundler(5, 5)

	merger := NewMerger(bundler, time.Second, "", nil, nil, nil, nil, nil, "")

	mergeFiles := map[uint64][]*bundle.OneBlockFile{
		0: {
			bundle.MustNewMergedOneBlockFile("0000000001-20210728T105016.01-00000001a-00000000a-0-suffix"),
			bundle.MustNewMergedOneBlockFile("0000000002-20210728T105016.02-00000002a-00000001a-0-suffix"),
			bundle.MustNewMergedOneBlockFile("0000000003-20210728T105016.03-00000003a-00000002a-0-suffix"),
			bundle.MustNewMergedOneBlockFile("0000000004-20210728T105016.06-00000004a-00000003a-2-suffix"),
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
		bundle.MustNewOneBlockFile("0000000001-20210728T105016.01-00000001a-00000000a-0-suffix"),
		bundle.MustNewOneBlockFile("0000000002-20210728T105016.02-00000002a-00000001a-0-suffix"),
		bundle.MustNewOneBlockFile("0000000003-20210728T105016.03-00000003a-00000002a-0-suffix"),
		bundle.MustNewOneBlockFile("0000000004-20210728T105016.06-00000004a-00000003a-0-suffix"),
		bundle.MustNewOneBlockFile("0000000006-20210728T105016.08-00000006a-00000004a-0-suffix"),
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

	err = merger.launch()
	require.NoError(t, err)

	expectedDeleted := mergedFiles //normal purge and too old file
	require.Equal(t, bundle.ToSortedIDs(expectedDeleted), bundle.ToSortedIDs(deletedFiles))

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

func TestMerger_Launch_FailFetchOneBlockFiles(t *testing.T) {
	bundler := bundle.NewBundler(5, 5)

	fetchMergedFiles := func(lowBlockNum uint64) ([]*bundle.OneBlockFile, error) { return []*bundle.OneBlockFile{}, nil }
	fetchOneBlockFiles := func(ctx context.Context) (oneBlockFiles []*bundle.OneBlockFile, err error) {
		return nil, fmt.Errorf("couldn't fetch one block files")
	}
	merger := NewMerger(bundler, 0, "", fetchMergedFiles, fetchOneBlockFiles, nil, nil, nil, "")

	merger.Launch()
}

func TestMerger_Launch_Drift(t *testing.T) {
	c := struct {
		name                      string
		files                     []*bundle.OneBlockFile
		blockLimit                uint64
		expectedHighestBlockLimit uint64
		expectedLastMergeBlockID  string
	}{
		name: "should call",
		files: []*bundle.OneBlockFile{
			bundle.MustNewOneBlockFile("0000000114-20210728T105016.0-00000114a-00000113a-90-suffix"),
			bundle.MustNewOneBlockFile("0000000115-20210728T105116.0-00000115a-00000114a-90-suffix"),
			bundle.MustNewOneBlockFile("0000000116-20210728T105216.0-00000116a-00000115a-90-suffix"),
			bundle.MustNewOneBlockFile("0000000117-20210728T105316.0-00000117a-00000116a-90-suffix"),
			bundle.MustNewOneBlockFile("0000000121-20210728T105416.0-00000121a-00000117b-90-suffix"),
		},
		blockLimit:                120,
		expectedHighestBlockLimit: 117,
		expectedLastMergeBlockID:  "00000117a",
	}

	bundler := bundle.NewBundler(10, c.blockLimit)

	for _, f := range c.files {
		bundler.AddOneBlockFile(f)
	}

	bundler.Commit(c.blockLimit)

	var areWeDoneYet uint64
	done := make(chan struct{})
	fetchMergedFiles := func(lowBlockNum uint64) ([]*bundle.OneBlockFile, error) {
		if areWeDoneYet == 1 {
			close(done)
		}
		areWeDoneYet += 1
		return []*bundle.OneBlockFile{}, nil
	}

	fetchOneBlockFiles := func(ctx context.Context) (oneBlockFiles []*bundle.OneBlockFile, err error) {
		return c.files, nil
	}

	merger := NewMerger(bundler, 0, "", fetchMergedFiles, fetchOneBlockFiles, nil, nil, nil, "")
	go merger.Launch()
	select {
	case <-time.After(time.Second):
		t.Fail()
	case <-done:
		merger.Shutdown(nil)
	}
}
