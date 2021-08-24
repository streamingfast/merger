package merger

import (
	"errors"
	"testing"

	"github.com/streamingfast/bstream"

	"github.com/streamingfast/bstream/forkable"

	"github.com/stretchr/testify/require"
)

//                                  |                           |                                  |                           |
// 100a - 101a - 102a - 103a - 104a - 106a - 107a - 108a - 109a - 110a - 111a - 112a - 113a - 114a - 115a - 116a - 117a - 118a - 120a
//            \- 102b - 103b                     \- 108b - 109b - 110b
//                                                             \- 110c - 111c

//File 0
//	100a - 101a - 102a - 103a - 104a
//             \- 102b - 103b

//File 5 with skipped block 5a
//	106a - 107a - 108a - 109a
//             \- 108b - 109b

//File 10 with multiple block 10
// 	  110a - 111a - 112a - 113a - 114a
// \- 110b
// \- 110c - 111c

//File 15 with missing last block 19
// 115a - 116a - 117a - 118a

//"0000000100-20210728T105016.0-00000100a-00000099a",
//"0000000101-20210728T105016.0-00000101a-00000100a",
//"0000000102-20210728T105016.0-00000102a-00000101a",
//"0000000102-20210728T105016.0-00000102b-00000101a",
//"0000000103-20210728T105016.0-00000103b-00000102b",
//"0000000103-20210728T105016.0-00000103a-00000102a",
//"0000000104-20210728T105016.0-00000104a-00000103a",
//"0000000106-20210728T105016.0-00000106a-00000104a",
//"0000000107-20210728T105016.0-00000107a-00000106a",
//"0000000108-20210728T105016.0-00000108b-00000107a",
//"0000000109-20210728T105016.0-00000109b-00000108b",
//"0000000110-20210728T105016.0-00000110b-00000109b",
//"0000000110-20210728T105016.0-00000110c-00000109b",
//"0000000111-20210728T105016.0-00000111c-00000110c",
//"0000000108-20210728T105016.0-00000108a-00000107a",
//"0000000109-20210728T105016.0-00000109a-00000108a",
//"0000000110-20210728T105016.0-00000110a-00000109a",
//"0000000111-20210728T105016.0-00000111a-00000110a",
//"0000000112-20210728T105016.0-00000112a-00000111a",
//"0000000113-20210728T105016.0-00000113a-00000112a",
//"0000000114-20210728T105016.0-00000114a-00000113a",
//"0000000115-20210728T105016.0-00000115a-00000114a",
//"0000000116-20210728T105016.0-00000116a-00000115a",
//"0000000117-20210728T105016.0-00000117a-00000116a",
//"0000000118-20210728T105016.0-00000118a-00000117a",
//"0000000120-20210728T105016.0-00000120a-00000118a",

func TestBundler_IsComplete(t *testing.T) {

	cases := []struct {
		name                       string
		files                      []string
		lastMergeBlockID           string
		blockLimit                 uint64
		expectedCompleted          bool
		expectedLowerBlockNumLimit uint64
		expectedHighestBlockLimit  uint64
	}{
		{
			name: "file 0",
			files: []string{
				"0000000100-20210728T105016.0-00000100a-00000099a",
				"0000000101-20210728T105016.0-00000101a-00000100a",
				"0000000102-20210728T105016.0-00000102a-00000101a",
				"0000000102-20210728T105016.0-00000102b-00000101a",
				"0000000103-20210728T105016.0-00000103b-00000102b",
				"0000000103-20210728T105016.0-00000103a-00000102a",
				"0000000104-20210728T105016.0-00000104a-00000103a",
				"0000000106-20210728T105016.0-00000106a-00000104a",
			},
			lastMergeBlockID:  "00000099a",
			blockLimit:        105,
			expectedCompleted: true,
			//expectedLowerBlockNumLimit: 100,
			expectedHighestBlockLimit: 104,
		},
		{
			name: "file 0 incomplete",
			files: []string{
				"0000000100-20210728T105016.0-00000100a-00000099a",
				"0000000101-20210728T105016.0-00000101a-00000100a",
				"0000000102-20210728T105016.0-00000102a-00000101a",
				"0000000102-20210728T105016.0-00000102b-00000101a",
				"0000000103-20210728T105016.0-00000103b-00000102b",
				"0000000103-20210728T105016.0-00000103a-00000102a",
				"0000000104-20210728T105016.0-00000104a-00000103a",
			},
			lastMergeBlockID:  "00000099a",
			blockLimit:        105,
			expectedCompleted: false,
			//expectedLowerBlockNumLimit: 0,
			expectedHighestBlockLimit: 0,
		},
		{
			name: "file 0 no longest chain",
			files: []string{
				"0000000100-20210728T105016.0-00000100a-00000099a",
				"0000000101-20210728T105016.0-00000101a-00000100a",
				"0000000102-20210728T105016.0-00000102a-00000101a",
				"0000000102-20210728T105016.0-00000102b-00000101a",
				"0000000103-20210728T105016.0-00000103b-00000102b",
				"0000000103-20210728T105016.0-00000103a-00000102a",
			},
			lastMergeBlockID:  "00000099a",
			blockLimit:        105,
			expectedCompleted: false,
			//expectedLowerBlockNumLimit: 0,
			expectedHighestBlockLimit: 0,
		},
		{
			name: "file 5",
			files: []string{
				"0000000106-20210728T105016.0-00000106a-00000104a",
				"0000000107-20210728T105016.0-00000107a-00000106a",
				"0000000108-20210728T105016.0-00000108b-00000107a",
				"0000000109-20210728T105016.0-00000109b-00000108b",
				"0000000110-20210728T105016.0-00000110b-00000109b",
				"0000000110-20210728T105016.0-00000110c-00000109b",
				"0000000111-20210728T105016.0-00000111c-00000110c",
				"0000000108-20210728T105016.0-00000108a-00000107a",
				"0000000109-20210728T105016.0-00000109a-00000108a",
				"0000000110-20210728T105016.0-00000110a-00000109a",
				"0000000111-20210728T105016.0-00000111a-00000110a",
				"0000000112-20210728T105016.0-00000112a-00000111a",
			},
			lastMergeBlockID:  "00000104a",
			blockLimit:        110,
			expectedCompleted: true,
			//expectedLowerBlockNumLimit: 106,
			expectedHighestBlockLimit: 109,
		},
		{
			name: "file 10",
			files: []string{
				"0000000107-20210728T105016.1-00000107a-00000106a",
				"0000000108-20210728T105016.2-00000108b-00000107a",
				"0000000109-20210728T105016.3-00000109b-00000108b",
				"0000000110-20210728T105016.4-00000110b-00000109b",
				"0000000110-20210728T105016.5-00000110c-00000109b",
				"0000000111-20210728T105016.6-00000111c-00000110c",
				"0000000108-20210728T105016.7-00000108a-00000107a",
				"0000000109-20210728T105016.8-00000109a-00000108a",
				"0000000110-20210728T105016.9-00000110a-00000109a",
				"0000000111-20210728T105016.10-00000111a-00000110a",
				"0000000112-20210728T105016.11-00000112a-00000111a",
				"0000000113-20210728T105016.12-00000113a-00000112a",
				"0000000114-20210728T105016.13-00000114a-00000113a",
				"0000000115-20210728T105016.14-00000115a-00000114a",
			},
			lastMergeBlockID:  "00000109a",
			blockLimit:        115,
			expectedCompleted: true,
			//expectedLowerBlockNumLimit: 110,
			expectedHighestBlockLimit: 114,
		},
		{
			name: "file 15",
			files: []string{
				"0000000115-20210728T105016.0-00000115a-00000114a",
				"0000000116-20210728T105016.0-00000116a-00000115a",
				"0000000117-20210728T105016.0-00000117a-00000116a",
				"0000000118-20210728T105016.0-00000118a-00000117a",
				"0000000120-20210728T105016.0-00000120a-00000118a",
			},
			lastMergeBlockID:  "00000114a",
			blockLimit:        120,
			expectedCompleted: true,
			//expectedLowerBlockNumLimit: 115,
			expectedHighestBlockLimit: 118,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			bundler := NewBundler(5, 100, c.blockLimit, "")
			bundler.lastMergeBlock = &forkable.Block{
				BlockID: c.lastMergeBlockID,
			}
			for _, f := range c.files {
				bundler.AddOneBlockFile(MustTestNewOneBlockFile(f))
			}
			completed, highestBlockLimit := bundler.isComplete()
			require.Equal(t, c.expectedCompleted, completed)
			require.Equal(t, c.expectedHighestBlockLimit, highestBlockLimit)
		})
	}

}

func MustTestNewOneBlockFile(fileName string) *OneBlockFile {
	blockNum, blockTime, blockID, previousBlockID, canonicalName, err := parseFilename(fileName)
	if err != nil {
		panic(err)
	}
	return &OneBlockFile{
		canonicalName: canonicalName,
		filenames: map[string]struct{}{
			fileName: Empty,
		},
		blockTime:  blockTime,
		id:         blockID,
		num:        blockNum,
		previousID: previousBlockID,
	}
}
func MustTestNewMergedOneBlockFile(fileName string) *OneBlockFile {
	fi := MustTestNewOneBlockFile(fileName)
	fi.merged = true
	return fi
}

func TestBundler_MergeableFiles(t *testing.T) {
	cases := []struct {
		name                     string
		files                    []*OneBlockFile
		lastMergeBlockID         string
		blockLimit               uint64
		expectedIDs              []string
		expectedLastMergeBlockID string
	}{
		{
			name: "file 0",
			files: []*OneBlockFile{
				MustTestNewOneBlockFile("0000000100-20210728T105016.01-00000100a-00000099a"),
				MustTestNewOneBlockFile("0000000101-20210728T105016.02-00000101a-00000100a"),
				MustTestNewOneBlockFile("0000000102-20210728T105016.03-00000102a-00000101a"),
				MustTestNewOneBlockFile("0000000102-20210728T105016.04-00000102b-00000101a"),
				MustTestNewOneBlockFile("0000000103-20210728T105016.05-00000103b-00000102b"),
				MustTestNewOneBlockFile("0000000103-20210728T105016.06-00000103a-00000102a"),
				MustTestNewOneBlockFile("0000000104-20210728T105016.07-00000104a-00000103a"),
				MustTestNewOneBlockFile("0000000106-20210728T105016.08-00000106a-00000104a"),
			},
			lastMergeBlockID:         "00000099a",
			blockLimit:               105,
			expectedIDs:              []string{"00000100a", "00000101a", "00000102a", "00000102b", "00000103b", "00000103a", "00000104a"},
			expectedLastMergeBlockID: "00000104a",
		},
		{
			name: "file 5",
			files: []*OneBlockFile{
				MustTestNewOneBlockFile("0000000106-20210728T105016.01-00000106a-00000104a"),
				MustTestNewOneBlockFile("0000000107-20210728T105016.02-00000107a-00000106a"),
				MustTestNewOneBlockFile("0000000108-20210728T105016.03-00000108b-00000107a"),
				MustTestNewOneBlockFile("0000000109-20210728T105016.04-00000109b-00000108b"),
				MustTestNewOneBlockFile("0000000110-20210728T105016.05-00000110b-00000109b"),
				MustTestNewOneBlockFile("0000000110-20210728T105016.06-00000110c-00000109b"),
				MustTestNewOneBlockFile("0000000111-20210728T105016.07-00000111c-00000110c"),
				MustTestNewOneBlockFile("0000000108-20210728T105016.08-00000108a-00000107a"),
				MustTestNewOneBlockFile("0000000109-20210728T105016.09-00000109a-00000108a"),
				MustTestNewOneBlockFile("0000000110-20210728T105016.10-00000110a-00000109a"),
				MustTestNewOneBlockFile("0000000111-20210728T105016.11-00000111a-00000110a"),
				MustTestNewOneBlockFile("0000000112-20210728T105016.12-00000112a-00000111a"),
			},
			lastMergeBlockID:         "00000104a",
			blockLimit:               110,
			expectedIDs:              []string{"00000106a", "00000107a", "00000108b", "00000109b", "00000108a", "00000109a"},
			expectedLastMergeBlockID: "00000109a",
		},
		{
			name: "file 10",
			files: []*OneBlockFile{
				MustTestNewMergedOneBlockFile("0000000107-20210728T105016.01-00000107a-00000106a"),
				MustTestNewMergedOneBlockFile("0000000108-20210728T105016.02-00000108b-00000107a"),
				MustTestNewMergedOneBlockFile("0000000109-20210728T105016.03-00000109b-00000108b"),
				MustTestNewOneBlockFile("0000000110-20210728T105016.04-00000110b-00000109b"),
				MustTestNewOneBlockFile("0000000110-20210728T105016.05-00000110c-00000109b"),
				MustTestNewOneBlockFile("0000000111-20210728T105016.06-00000111c-00000110c"),
				MustTestNewMergedOneBlockFile("0000000108-20210728T105016.07-00000108a-00000107a"),
				MustTestNewMergedOneBlockFile("0000000109-20210728T105016.08-00000109a-00000108a"),
				MustTestNewOneBlockFile("0000000110-20210728T105016.09-00000110a-00000109a"),
				MustTestNewOneBlockFile("0000000111-20210728T105016.10-00000111a-00000110a"),
				MustTestNewOneBlockFile("0000000112-20210728T105016.11-00000112a-00000111a"),
				MustTestNewOneBlockFile("0000000113-20210728T105016.12-00000113a-00000112a"),
				MustTestNewOneBlockFile("0000000114-20210728T105016.13-00000114a-00000113a"),
				MustTestNewOneBlockFile("0000000115-20210728T105016.14-00000115a-00000114a"),
			},
			lastMergeBlockID:         "00000109a",
			blockLimit:               115,
			expectedIDs:              []string{"00000110b", "00000110c", "00000111c", "00000110a", "00000111a", "00000112a", "00000113a", "00000114a"},
			expectedLastMergeBlockID: "00000114a",
		},
		{
			name: "file 15",
			files: []*OneBlockFile{
				MustTestNewOneBlockFile("0000000115-20210728T105016.0-00000115a-00000114a"),
				MustTestNewOneBlockFile("0000000116-20210728T105016.0-00000116a-00000115a"),
				MustTestNewOneBlockFile("0000000117-20210728T105016.0-00000117a-00000116a"),
				MustTestNewOneBlockFile("0000000118-20210728T105016.0-00000118a-00000117a"),
				MustTestNewOneBlockFile("0000000120-20210728T105016.0-00000120a-00000118a"),
			},
			lastMergeBlockID:         "00000114a",
			blockLimit:               120,
			expectedIDs:              []string{"00000115a", "00000116a", "00000117a", "00000118a"},
			expectedLastMergeBlockID: "00000118a",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			bundler := NewBundler(5, 100, c.blockLimit, "")
			bundler.lastMergeBlock = &forkable.Block{BlockID: c.lastMergeBlockID}
			for _, f := range c.files {
				bundler.AddOneBlockFile(f)
			}
			completed, highestBlockLimit := bundler.isComplete()
			require.True(t, completed)
			mergeableFiles, err := bundler.ToBundle(highestBlockLimit)
			require.NoError(t, err)
			err = bundler.Commit(highestBlockLimit)
			require.NoError(t, err)

			ids := toIDs(mergeableFiles)
			require.Equal(t, c.expectedIDs, ids)
			require.Equal(t, c.expectedLastMergeBlockID, bundler.lastMergeBlock.BlockID)
		})
	}
}
func toIDs(oneBlockFileList []*OneBlockFile) (ids []string) {
	for _, file := range oneBlockFileList {
		ids = append(ids, file.id)
	}
	return ids
}

func TestBundler_Complexe(t *testing.T) {

	files := []*OneBlockFile{
		MustTestNewOneBlockFile("0000000100-20210728T105016.01-00000100a-00000099a"),
		MustTestNewOneBlockFile("0000000101-20210728T105016.02-00000101a-00000100a"),
		MustTestNewOneBlockFile("0000000102-20210728T105016.03-00000102a-00000101a"),
		MustTestNewOneBlockFile("0000000102-20210728T105016.04-00000102b-00000101a"),
		MustTestNewOneBlockFile("0000000103-20210728T105016.05-00000103b-00000102b"),
		MustTestNewOneBlockFile("0000000103-20210728T105016.06-00000103a-00000102a"),
		MustTestNewOneBlockFile("0000000104-20210728T105016.07-00000104a-00000103a"),
		MustTestNewOneBlockFile("0000000106-20210728T105016.08-00000106a-00000104a"),
		MustTestNewOneBlockFile("0000000107-20210728T105016.09-00000107a-00000106a"),
		MustTestNewOneBlockFile("0000000108-20210728T105016.10-00000108b-00000107a"),
		MustTestNewOneBlockFile("0000000109-20210728T105016.11-00000109b-00000108b"),
		MustTestNewOneBlockFile("0000000110-20210728T105016.12-00000110b-00000109b"),
		MustTestNewOneBlockFile("0000000110-20210728T105016.13-00000110c-00000109b"),
		MustTestNewOneBlockFile("0000000111-20210728T105016.14-00000111c-00000110c"),
		MustTestNewOneBlockFile("0000000108-20210728T105016.15-00000108a-00000107a"),
		MustTestNewOneBlockFile("0000000109-20210728T105016.16-00000109a-00000108a"),
		MustTestNewOneBlockFile("0000000110-20210728T105016.17-00000110a-00000109a"),
		MustTestNewOneBlockFile("0000000111-20210728T105016.18-00000111a-00000110a"),
		MustTestNewOneBlockFile("0000000112-20210728T105016.19-00000112a-00000111a"),
		MustTestNewOneBlockFile("0000000113-20210728T105016.20-00000113a-00000112a"),
		MustTestNewOneBlockFile("0000000114-20210728T105016.21-00000114a-00000113a"),
		MustTestNewOneBlockFile("0000000115-20210728T105016.22-00000115a-00000114a"),
		MustTestNewOneBlockFile("0000000116-20210728T105016.23-00000116a-00000115a"),
		MustTestNewOneBlockFile("0000000117-20210728T105016.24-00000117a-00000116a"),
		MustTestNewOneBlockFile("0000000118-20210728T105016.25-00000118a-00000117a"),
		MustTestNewOneBlockFile("0000000120-20210728T105016.26-00000120a-00000118a"),
	}

	bundler := NewBundler(5, 100, 105, "")
	bundler.lastMergeBlock = &forkable.Block{BlockID: "00000099a"}
	for _, f := range files {
		bundler.AddOneBlockFile(f)
	}

	completed, highestBlockLimit := bundler.isComplete()
	require.True(t, completed)
	mergeableFiles, err := bundler.ToBundle(highestBlockLimit)
	require.NoError(t, err)
	err = bundler.Commit(highestBlockLimit)
	require.NoError(t, err)

	ids := toIDs(mergeableFiles)
	require.Equal(t, []string{"00000100a", "00000101a", "00000102a", "00000102b", "00000103b", "00000103a", "00000104a"}, ids)

	completed, highestBlockLimit = bundler.isComplete()
	require.True(t, completed)
	mergeableFiles, err = bundler.ToBundle(highestBlockLimit)
	require.NoError(t, err)
	err = bundler.Commit(highestBlockLimit)
	require.NoError(t, err)
	ids = toIDs(mergeableFiles)
	require.Equal(t, []string{"00000106a", "00000107a", "00000108b", "00000109b", "00000108a", "00000109a"}, ids)

	completed, highestBlockLimit = bundler.isComplete()
	require.True(t, completed)
	mergeableFiles, err = bundler.ToBundle(highestBlockLimit)
	require.NoError(t, err)
	err = bundler.Commit(highestBlockLimit)
	require.NoError(t, err)

	ids = toIDs(mergeableFiles)
	require.Equal(t, []string{"00000110b", "00000110c", "00000111c", "00000110a", "00000111a", "00000112a", "00000113a", "00000114a"}, ids)

	completed, highestBlockLimit = bundler.isComplete()
	require.True(t, completed)
	mergeableFiles, err = bundler.ToBundle(highestBlockLimit)
	require.NoError(t, err)
	err = bundler.Commit(highestBlockLimit)
	require.NoError(t, err)

	ids = toIDs(mergeableFiles)
	require.Equal(t, []string{"00000115a", "00000116a", "00000117a", "00000118a"}, ids)
}

func TestBundler_BackToTheFuture(t *testing.T) {
	// load forkdb with some history ...
	files := []*OneBlockFile{
		MustTestNewMergedOneBlockFile("0000000094-20210728T105016.01-00000094a-00000093a"),
		MustTestNewMergedOneBlockFile("0000000095-20210728T105016.01-00000095a-00000094a"),
		MustTestNewMergedOneBlockFile("0000000096-20210728T105016.01-00000096a-00000095a"),
		MustTestNewMergedOneBlockFile("0000000097-20210728T105016.01-00000097a-00000096a"),
		MustTestNewMergedOneBlockFile("0000000098-20210728T105016.01-00000098a-00000097a"),
		MustTestNewMergedOneBlockFile("0000000099-20210728T105016.01-00000099a-00000098a"),

		MustTestNewOneBlockFile("0000000100-20210728T105016.01-00000100a-00000099a"),
		MustTestNewOneBlockFile("0000000101-20210728T105016.02-00000101a-00000100a"),
		MustTestNewOneBlockFile("0000000102-20210728T105016.03-00000102a-00000101a"),
		MustTestNewOneBlockFile("0000000103-20210728T105016.06-00000103a-00000102a"),
		MustTestNewOneBlockFile("0000000104-20210728T105016.07-00000104a-00000103a"),
		MustTestNewOneBlockFile("0000000106-20210728T105016.08-00000106a-00000104a"),
	}

	bundler := NewBundler(5, 100, 105, "")
	bundler.lastMergeBlock = &forkable.Block{BlockID: "00000099a"}
	for _, f := range files {
		bundler.AddOneBlockFile(f)
	}

	// Let's merge a first back of block from 100 to 104
	completed, highestBlockLimit := bundler.isComplete()
	require.True(t, completed)
	mergeableFiles, err := bundler.ToBundle(highestBlockLimit)
	require.NoError(t, err)
	ids := toIDs(mergeableFiles)
	require.Equal(t, []string{"00000100a", "00000101a", "00000102a", "00000103a", "00000104a"}, ids)
	err = bundler.Commit(highestBlockLimit)
	require.NoError(t, err)
	// Add a very old file
	bundler.AddOneBlockFile(MustTestNewOneBlockFile("000000095-20210728T105015.01-00000095b-00000094a"))
	require.NoError(t, err)

	//that new file should not trigger a merge
	completed, highestBlockLimit = bundler.isComplete()
	require.False(t, completed)

	// Add missing file for this back in time fork
	bundler.AddOneBlockFile(MustTestNewOneBlockFile("000000096-20210728T105015.02-00000096b-00000095b"))
	bundler.AddOneBlockFile(MustTestNewOneBlockFile("000000097-20210728T105015.03-00000097b-00000096b"))
	bundler.AddOneBlockFile(MustTestNewOneBlockFile("000000098-20210728T105015.04-00000098b-00000097b"))
	bundler.AddOneBlockFile(MustTestNewOneBlockFile("000000099-20210728T105015.05-00000099b-00000098b"))
	bundler.AddOneBlockFile(MustTestNewOneBlockFile("000000100-20210728T105015.06-00000100b-00000099b"))
	bundler.AddOneBlockFile(MustTestNewOneBlockFile("000000101-20210728T105015.07-00000101b-00000100b"))
	bundler.AddOneBlockFile(MustTestNewOneBlockFile("000000102-20210728T105015.08-00000102b-00000101b"))
	bundler.AddOneBlockFile(MustTestNewOneBlockFile("000000103-20210728T105015.09-00000103b-00000102b"))
	bundler.AddOneBlockFile(MustTestNewOneBlockFile("000000104-20210728T105015.10-00000104b-00000103b"))
	bundler.AddOneBlockFile(MustTestNewOneBlockFile("000000105-20210728T105015.11-00000105b-00000104b"))
	bundler.AddOneBlockFile(MustTestNewOneBlockFile("000000106-20210728T105015.12-00000106b-00000105b"))
	bundler.AddOneBlockFile(MustTestNewOneBlockFile("000000107-20210728T105015.12-00000107b-00000106b"))
	bundler.AddOneBlockFile(MustTestNewOneBlockFile("000000108-20210728T105015.12-00000108b-00000107b"))
	bundler.AddOneBlockFile(MustTestNewOneBlockFile("000000109-20210728T105015.12-00000109b-00000108b"))

	completed, highestBlockLimit = bundler.isComplete()
	//should not trigger merge yet
	require.False(t, completed)

	bundler.AddOneBlockFile(MustTestNewOneBlockFile("000000110-20210728T105015.12-00000110b-00000109b"))

	completed, highestBlockLimit = bundler.isComplete()
	//here we go!
	require.True(t, completed)

	mergeableFiles, err = bundler.ToBundle(highestBlockLimit)
	require.NoError(t, err)
	ids = toIDs(mergeableFiles)
	require.Equal(t, []string{
		"00000095b", "00000096b",
		"00000097b", "00000098b",
		"00000099b", "00000100b",
		"00000101b", "00000102b",
		"00000103b", "00000104b",
		"00000105b", "00000106b",
		"00000107b", "00000108b",
		"00000109b", "00000106a"}, ids)
}

func TestBundler_purge(t *testing.T) {

	largeFileSet := []*OneBlockFile{
		MustTestNewOneBlockFile("0000000100-20210728T105016.01-00000100a-00000099a"),
		MustTestNewOneBlockFile("0000000101-20210728T105016.02-00000101a-00000100a"),
		MustTestNewOneBlockFile("0000000102-20210728T105016.03-00000102a-00000101a"),
		MustTestNewOneBlockFile("0000000102-20210728T105016.04-00000102b-00000101a"),
		MustTestNewOneBlockFile("0000000103-20210728T105016.05-00000103b-00000102b"),
		MustTestNewOneBlockFile("0000000103-20210728T105016.06-00000103a-00000102a"),
		MustTestNewOneBlockFile("0000000104-20210728T105016.07-00000104a-00000103a"),
		MustTestNewOneBlockFile("0000000106-20210728T105016.08-00000106a-00000104a"),
		MustTestNewOneBlockFile("0000000107-20210728T105016.09-00000107a-00000106a"),
		MustTestNewOneBlockFile("0000000108-20210728T105016.10-00000108b-00000107a"),
		MustTestNewOneBlockFile("0000000109-20210728T105016.11-00000109b-00000108b"),
		MustTestNewOneBlockFile("0000000110-20210728T105016.12-00000110b-00000109b"),
		MustTestNewOneBlockFile("0000000110-20210728T105016.13-00000110c-00000109b"),
		MustTestNewOneBlockFile("0000000111-20210728T105016.14-00000111c-00000110c"),
		MustTestNewOneBlockFile("0000000108-20210728T105016.15-00000108a-00000107a"),
		MustTestNewOneBlockFile("0000000109-20210728T105016.16-00000109a-00000108a"),
		MustTestNewOneBlockFile("0000000110-20210728T105016.17-00000110a-00000109a"),
		MustTestNewOneBlockFile("0000000111-20210728T105016.18-00000111a-00000110a"),
		MustTestNewOneBlockFile("0000000112-20210728T105016.19-00000112a-00000111a"),
		MustTestNewOneBlockFile("0000000113-20210728T105016.20-00000113a-00000112a"),
		MustTestNewOneBlockFile("0000000114-20210728T105016.21-00000114a-00000113a"),
		MustTestNewOneBlockFile("0000000115-20210728T105016.22-00000115a-00000114a"),
		MustTestNewOneBlockFile("0000000116-20210728T105016.23-00000116a-00000115a"),
		MustTestNewOneBlockFile("0000000117-20210728T105016.24-00000117a-00000116a"),
		MustTestNewOneBlockFile("0000000118-20210728T105016.25-00000118a-00000117a"),
		MustTestNewOneBlockFile("0000000120-20210728T105016.26-00000120a-00000118a"),
	}

	//                                  |                           |                                  |                           |
	// 100a - 101a - 102a - 103a - 104a - 106a - 107a - 108a - 109a - 110a - 111a - 112a - 113a - 114a - 115a - 116a - 117a - 118a - 120a
	//            \- 102b - 103b                     \- 108b - 109b - 110b
	//                                                             \- 110c - 111c

	cases := []struct {
		name                      string
		files                     []*OneBlockFile
		upToBlock                 uint64
		expectedTreeSize          int
		expectedPurgedFileCount   int
		expectedLongestFirstBlock string
	}{
		{
			name:                      "longest chain",
			files:                     largeFileSet,
			upToBlock:                 100,
			expectedLongestFirstBlock: "00000101a",
			expectedTreeSize:          25,
			expectedPurgedFileCount:   1,
		},
		{
			name:                      "None purgeable fork",
			files:                     largeFileSet,
			upToBlock:                 102,
			expectedLongestFirstBlock: "00000101a",
			expectedTreeSize:          25,
			expectedPurgedFileCount:   1,
		},
		{
			name:                      "Purging past fork",
			files:                     largeFileSet,
			upToBlock:                 104,
			expectedLongestFirstBlock: "00000106a",
			expectedTreeSize:          19,
			expectedPurgedFileCount:   7,
		},
		{
			name:                      "Purging right at fork end",
			files:                     largeFileSet,
			upToBlock:                 103,
			expectedLongestFirstBlock: "00000104a",
			expectedTreeSize:          20,
			expectedPurgedFileCount:   6,
		},
		{
			name:                      "Purging middle of second fork",
			files:                     largeFileSet,
			upToBlock:                 109,
			expectedLongestFirstBlock: "00000107a",
			expectedTreeSize:          18,
			expectedPurgedFileCount:   8,
		},
		{
			name:                      "Purging middle of second fork fork",
			files:                     largeFileSet,
			upToBlock:                 110,
			expectedLongestFirstBlock: "00000107a",
			expectedTreeSize:          18,
			expectedPurgedFileCount:   8,
		},
		{
			name:                      "Purging on end of second fork",
			files:                     largeFileSet,
			upToBlock:                 111,
			expectedLongestFirstBlock: "00000112a",
			expectedTreeSize:          8,
			expectedPurgedFileCount:   18,
		},
		{
			name:                      "Purging all",
			files:                     largeFileSet,
			upToBlock:                 120,
			expectedLongestFirstBlock: "",
			expectedTreeSize:          0,
			expectedPurgedFileCount:   26,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			bundler := NewBundler(5, 100, 105, "")
			for _, f := range c.files {
				bundler.AddOneBlockFile(f)
			}

			err := bundler.purge(c.upToBlock, func(purgedOneBlockFiles []*OneBlockFile) {
				require.Equal(t, c.expectedPurgedFileCount, len(purgedOneBlockFiles))
			})
			require.NoError(t, err)

			_, err = bundler.db.Roots()

			if c.expectedLongestFirstBlock == "" && c.expectedTreeSize == 0 {
				require.Errorf(t, err, "no link")
				return
			}
			require.NoError(t, err)

			tree, err := bundler.getTree()
			require.NoError(t, err) //this will mean that we created multiple root.
			longest := tree.Chains().LongestChain()
			require.Equal(t, c.expectedLongestFirstBlock, longest[0])
			require.Equal(t, c.expectedTreeSize, tree.Size())
		})
	}
}
func TestBundler_Purge(t *testing.T) {

	largeFileSet := []*OneBlockFile{
		MustTestNewOneBlockFile("0000000100-20210728T105016.01-00000100a-00000099a"),
		MustTestNewOneBlockFile("0000000101-20210728T105016.02-00000101a-00000100a"),
		MustTestNewOneBlockFile("0000000102-20210728T105016.03-00000102a-00000101a"),
		MustTestNewOneBlockFile("0000000102-20210728T105016.04-00000102b-00000101a"),
		MustTestNewOneBlockFile("0000000103-20210728T105016.05-00000103b-00000102b"),
		MustTestNewOneBlockFile("0000000103-20210728T105016.06-00000103a-00000102a"),
		MustTestNewOneBlockFile("0000000104-20210728T105016.07-00000104a-00000103a"),
		MustTestNewOneBlockFile("0000000106-20210728T105016.08-00000106a-00000104a"),
		MustTestNewOneBlockFile("0000000107-20210728T105016.09-00000107a-00000106a"),
		MustTestNewOneBlockFile("0000000108-20210728T105016.10-00000108b-00000107a"),
		MustTestNewOneBlockFile("0000000109-20210728T105016.11-00000109b-00000108b"),
		MustTestNewOneBlockFile("0000000110-20210728T105016.12-00000110b-00000109b"),
		MustTestNewOneBlockFile("0000000110-20210728T105016.13-00000110c-00000109b"),
		MustTestNewOneBlockFile("0000000111-20210728T105016.14-00000111c-00000110c"),
		MustTestNewOneBlockFile("0000000108-20210728T105016.15-00000108a-00000107a"),
		MustTestNewOneBlockFile("0000000109-20210728T105016.16-00000109a-00000108a"),
		MustTestNewOneBlockFile("0000000110-20210728T105016.17-00000110a-00000109a"),
		MustTestNewOneBlockFile("0000000111-20210728T105016.18-00000111a-00000110a"),
		MustTestNewOneBlockFile("0000000112-20210728T105016.19-00000112a-00000111a"),
		MustTestNewOneBlockFile("0000000113-20210728T105016.20-00000113a-00000112a"),
		MustTestNewOneBlockFile("0000000114-20210728T105016.21-00000114a-00000113a"),
		MustTestNewOneBlockFile("0000000115-20210728T105016.22-00000115a-00000114a"),
		MustTestNewOneBlockFile("0000000116-20210728T105016.23-00000116a-00000115a"),
		MustTestNewOneBlockFile("0000000117-20210728T105016.24-00000117a-00000116a"),
		MustTestNewOneBlockFile("0000000118-20210728T105016.25-00000118a-00000117a"),
		MustTestNewOneBlockFile("0000000120-20210728T105016.26-00000120a-00000118a"),
	}

	//                                  |                           |                                  |                           |
	// 100a - 101a - 102a - 103a - 104a - 106a - 107a - 108a - 109a - 110a - 111a - 112a - 113a - 114a - 115a - 116a - 117a - 118a - 120a
	//            \- 102b - 103b                     \- 108b - 109b - 110b
	//                                                             \- 110c - 111c

	cases := []struct {
		name                      string
		files                     []*OneBlockFile
		expectedTreeSize          int
		expectedPurgedFileCount   int
		expectedLongestFirstBlock string
		maxFixableFork            uint64
	}{
		{
			name:                      "First block no fork",
			files:                     largeFileSet,
			maxFixableFork:            18, //100a
			expectedLongestFirstBlock: "00000101a",
			expectedTreeSize:          25,
			expectedPurgedFileCount:   1,
		},
		{
			name:                      "Only one block because of unpurgeable fork",
			files:                     largeFileSet,
			maxFixableFork:            16, //102a
			expectedLongestFirstBlock: "00000101a",
			expectedTreeSize:          25,
			expectedPurgedFileCount:   1,
		},
		{
			name:                      "end of fork",
			files:                     largeFileSet,
			maxFixableFork:            15, //103a
			expectedLongestFirstBlock: "00000104a",
			expectedTreeSize:          20,
			expectedPurgedFileCount:   6,
		},
		{
			name:                      "after fork",
			files:                     largeFileSet,
			maxFixableFork:            14, //104a
			expectedLongestFirstBlock: "00000106a",
			expectedTreeSize:          19,
			expectedPurgedFileCount:   7,
		},
		{
			name:                      "not enough",
			files:                     largeFileSet,
			maxFixableFork:            20, //nada
			expectedLongestFirstBlock: "00000100a",
			expectedTreeSize:          26,
			expectedPurgedFileCount:   0,
		},
		{
			name:                      "keep 1",
			files:                     largeFileSet,
			maxFixableFork:            1, //118a
			expectedLongestFirstBlock: "00000120a",
			expectedTreeSize:          1,
			expectedPurgedFileCount:   25,
		},
		{
			name:                      "remove all",
			files:                     largeFileSet,
			maxFixableFork:            0, //120a
			expectedLongestFirstBlock: "",
			expectedTreeSize:          0,
			expectedPurgedFileCount:   26,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			bundler := NewBundler(5, c.maxFixableFork, 105, "")
			for _, f := range c.files {
				bundler.AddOneBlockFile(f)
			}

			err := bundler.Purge(func(purgedOneBlockFiles []*OneBlockFile) {
				require.Equal(t, c.expectedPurgedFileCount, len(purgedOneBlockFiles))
			})
			require.NoError(t, err)

			_, err = bundler.db.Roots()

			if c.expectedLongestFirstBlock == "" && c.expectedTreeSize == 0 {
				require.Errorf(t, err, "no link")
				return
			}
			require.NoError(t, err)

			tree, err := bundler.getTree()
			require.NoError(t, err) //this will mean that we created multiple root.
			longest := tree.Chains().LongestChain()
			require.Equal(t, c.expectedLongestFirstBlock, longest[0])
			require.Equal(t, c.expectedTreeSize, tree.Size())
		})
	}
}

func TestBundler_Boostrap(t *testing.T) {
	mergeFiles := map[uint64][]*OneBlockFile{
		95: {
			MustTestNewOneBlockFile("0000000095-20210728T105016.07-00000095a-00000094a"),
			MustTestNewOneBlockFile("0000000096-20210728T105016.07-00000096a-00000095a"),
			MustTestNewOneBlockFile("0000000097-20210728T105016.07-00000097a-00000096a"),
			MustTestNewOneBlockFile("0000000098-20210728T105016.07-00000098a-00000097a"),
			MustTestNewOneBlockFile("0000000098-20210728T105016.07-00000098b-00000097a"),
			MustTestNewOneBlockFile("0000000099-20210728T105016.07-00000099a-00000098a"),
			MustTestNewOneBlockFile("0000000099-20210728T105016.07-00000099b-00000098b"),
		},
		100: {
			MustTestNewOneBlockFile("0000000100-20210728T105016.01-00000100a-00000099a"),
			MustTestNewOneBlockFile("0000000100-20210728T105016.01-00000100b-00000099b"),
			MustTestNewOneBlockFile("0000000101-20210728T105016.02-00000101a-00000100a"),
			MustTestNewOneBlockFile("0000000102-20210728T105016.03-00000102a-00000101a"),
			MustTestNewOneBlockFile("0000000103-20210728T105016.06-00000103a-00000102a"),
			MustTestNewOneBlockFile("0000000104-20210728T105016.07-00000104a-00000103a"),
		},
		105: {
			MustTestNewOneBlockFile("0000000106-20210728T105016.08-00000106a-00000104a"),
			MustTestNewOneBlockFile("0000000107-20210728T105016.09-00000107a-00000106a"),
			MustTestNewOneBlockFile("0000000108-20210728T105016.15-00000108a-00000107a"),
			MustTestNewOneBlockFile("0000000109-20210728T105016.16-00000109a-00000108a"),
		},
		110: {
			MustTestNewOneBlockFile("0000000110-20210728T105016.17-00000110a-00000109a"),
			MustTestNewOneBlockFile("0000000111-20210728T105016.18-00000111a-00000110a"),
			MustTestNewOneBlockFile("0000000112-20210728T105016.19-00000112a-00000111a"),
			MustTestNewOneBlockFile("0000000113-20210728T105016.20-00000113a-00000112a"),
			MustTestNewOneBlockFile("0000000114-20210728T105016.21-00000114a-00000113a"),
		},
		115: {
			MustTestNewOneBlockFile("0000000115-20210728T105016.22-00000115a-00000114a"),
			MustTestNewOneBlockFile("0000000109-20210728T105016.23-00000109b-00000108a"),
			MustTestNewOneBlockFile("0000000116-20210728T105016.24-00000116a-00000115a"),
			MustTestNewOneBlockFile("0000000117-20210728T105016.25-00000117a-00000116a"),
			MustTestNewOneBlockFile("0000000118-20210728T105016.26-00000118a-00000117a"),
		},
		120: {
			MustTestNewOneBlockFile("0000000120-20210728T105016.27-00000120a-00000118a"),
			MustTestNewOneBlockFile("0000000121-20210728T105016.28-00000121a-00000120a"),
			MustTestNewOneBlockFile("0000000122-20210728T105016.29-00000122a-00000121a"),
			MustTestNewOneBlockFile("0000000123-20210728T105016.30-00000123a-00000122a"),
			MustTestNewOneBlockFile("0000000124-20210728T105016.31-00000124a-00000123a"),
		},
	}

	testCases := []struct {
		name                            string
		firstExclusiveHighestBlockLimit uint64
		maxFixableFork                  uint64
		protocolFirstStreamableBlock    uint64
		expectedMergeFilesRead          []int
		expectedFirstBlockNum           uint64
		expectedErr                     bool
	}{
		{
			name:                            "Sunny path",
			firstExclusiveHighestBlockLimit: 125,
			maxFixableFork:                  100,
			protocolFirstStreamableBlock:    120,
			expectedMergeFilesRead:          []int{120},
			expectedFirstBlockNum:           120,
		},
		{
			name:                            "Fork over 2 merge files",
			firstExclusiveHighestBlockLimit: 120,
			maxFixableFork:                  5,
			protocolFirstStreamableBlock:    106,
			expectedMergeFilesRead:          []int{115, 110, 105},
			expectedFirstBlockNum:           108,
		},
		{
			name:                            "Fork over 1 merge file",
			firstExclusiveHighestBlockLimit: 105,
			maxFixableFork:                  5,
			protocolFirstStreamableBlock:    95,
			expectedMergeFilesRead:          []int{100, 95},
			expectedFirstBlockNum:           97,
		},
		{
			name:                            "missing merge file",
			firstExclusiveHighestBlockLimit: 120,
			maxFixableFork:                  100,
			protocolFirstStreamableBlock:    90, //this will trigger a missing merge file error
			expectedMergeFilesRead:          []int{115, 110, 105, 100, 95, 90},
			expectedFirstBlockNum:           95,
			expectedErr:                     true,
		},
	}

	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			bundler := NewBundler(5, c.maxFixableFork, c.firstExclusiveHighestBlockLimit, "")
			bstream.GetProtocolFirstStreamableBlock = c.protocolFirstStreamableBlock
			var mergeFileReads []int
			err := bundler.Boostrap(func(lowBlockNum uint64) ([]*OneBlockFile, error) {
				//this function feed block to bundler ...
				mergeFileReads = append(mergeFileReads, int(lowBlockNum))

				if oneBlockFiles, found := mergeFiles[lowBlockNum]; found {
					return oneBlockFiles, nil
				}
				return nil, errors.New("merge file not found")
			})

			if c.expectedErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			require.Equal(t, c.expectedMergeFilesRead, mergeFileReads)
			firstBlockNum, err := bundler.LongestChainFirstBlockNum()
			require.NoError(t, err)
			require.Equal(t, int(c.expectedFirstBlockNum), int(firstBlockNum))
		})
	}
}

func TestBundler_IsBlockTooOld(t *testing.T) {
	singleRootNoFork := []*OneBlockFile{
		MustTestNewOneBlockFile("0000000100-20210728T105016.01-00000100a-00000099a"),
		MustTestNewOneBlockFile("0000000101-20210728T105016.02-00000101a-00000100a"),
		MustTestNewOneBlockFile("0000000102-20210728T105016.03-00000102a-00000101a"),
		MustTestNewOneBlockFile("0000000103-20210728T105016.06-00000103a-00000102a"),
		MustTestNewOneBlockFile("0000000104-20210728T105016.07-00000104a-00000103a"),
		MustTestNewOneBlockFile("0000000106-20210728T105016.08-00000106a-00000104a"),
	}

	cases := []struct {
		name           string
		files          []*OneBlockFile
		blockNum       uint64
		maxFixableFork uint64
		expectedResult bool
	}{
		{
			name:           "in the middle",
			files:          singleRootNoFork,
			blockNum:       102,
			maxFixableFork: 100,
			expectedResult: false,
		},
		{
			name:           "in the future",
			files:          singleRootNoFork,
			blockNum:       200,
			maxFixableFork: 100,
			expectedResult: false,
		},
		{
			name:           "at first block",
			files:          singleRootNoFork,
			blockNum:       100,
			maxFixableFork: 100,
			expectedResult: false,
		},
		{
			name:           "before first block",
			files:          singleRootNoFork,
			blockNum:       99,
			maxFixableFork: 100,
			expectedResult: false,
		},
		{
			name:           "too old",
			files:          singleRootNoFork,
			blockNum:       5,
			maxFixableFork: 100,
			expectedResult: true,
		},
		{
			name:           "maxFixableFork capacity no reach",
			files:          singleRootNoFork,
			blockNum:       100,
			maxFixableFork: 1000,
			expectedResult: false,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			bundler := NewBundler(5, c.maxFixableFork, 105, "")
			for _, f := range c.files {
				bundler.AddOneBlockFile(f)
			}
			tooOld := bundler.IsBlockTooOld(c.blockNum)
			require.Equal(t, c.expectedResult, tooOld)

		})
	}
}

func TestBundler_IsBlockTooOld_MultipleRoot(t *testing.T) {
	singleRootNoFork := []*OneBlockFile{
		MustTestNewOneBlockFile("0000000100-20210728T105016.01-00000100a-00000099a"),
		MustTestNewOneBlockFile("0000000101-20210728T105016.02-00000101a-00000100a"),
		MustTestNewOneBlockFile("0000000102-20210728T105016.03-00000102a-00000101a"),
		//MustTestNewOneBlockFile("0000000103-20210728T105016.06-00000103a-00000102a"),
		MustTestNewOneBlockFile("0000000104-20210728T105016.07-00000104a-00000103a"),
		MustTestNewOneBlockFile("0000000106-20210728T105016.08-00000106a-00000104a"),
	}

	cases := []struct {
		name           string
		files          []*OneBlockFile
		blockNum       uint64
		maxFixableFork uint64
		expectedResult bool
	}{
		{
			name:           "in the middle",
			files:          singleRootNoFork,
			blockNum:       102,
			maxFixableFork: 100,
			expectedResult: false,
		},
		{
			name:           "in the future",
			files:          singleRootNoFork,
			blockNum:       200,
			maxFixableFork: 100,
			expectedResult: false,
		},
		{
			name:           "at first block",
			files:          singleRootNoFork,
			blockNum:       100,
			maxFixableFork: 100,
			expectedResult: false,
		},
		{
			name:           "before first block",
			files:          singleRootNoFork,
			blockNum:       99,
			maxFixableFork: 100,
			expectedResult: false,
		},
		{
			name:           "too old",
			files:          singleRootNoFork,
			blockNum:       5,
			maxFixableFork: 100,
			expectedResult: true,
		},
		{
			name:           "maxFixableFork capacity no reach",
			files:          singleRootNoFork,
			blockNum:       100,
			maxFixableFork: 1000,
			expectedResult: false,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			bundler := NewBundler(5, c.maxFixableFork, 105, "")
			for _, f := range c.files {
				bundler.AddOneBlockFile(f)
			}
			tooOld := bundler.IsBlockTooOld(c.blockNum)
			require.Equal(t, c.expectedResult, tooOld)

		})
	}
}
