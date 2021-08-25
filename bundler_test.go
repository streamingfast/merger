package merger

import (
	"errors"
	"testing"

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
				"0000000100-20210728T105016.0-00000100a-00000099a-90",
				"0000000101-20210728T105016.0-00000101a-00000100a-90",
				"0000000102-20210728T105016.0-00000102a-00000101a-90",
				"0000000102-20210728T105016.0-00000102b-00000101a-90",
				"0000000103-20210728T105016.0-00000103b-00000102b-90",
				"0000000103-20210728T105016.0-00000103a-00000102a-90",
				"0000000104-20210728T105016.0-00000104a-00000103a-90",
				"0000000106-20210728T105016.0-00000106a-00000104a-90",
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
				"0000000100-20210728T105016.0-00000100a-00000099a-90",
				"0000000101-20210728T105016.0-00000101a-00000100a-90",
				"0000000102-20210728T105016.0-00000102a-00000101a-90",
				"0000000102-20210728T105016.0-00000102b-00000101a-90",
				"0000000103-20210728T105016.0-00000103b-00000102b-90",
				"0000000103-20210728T105016.0-00000103a-00000102a-90",
				"0000000104-20210728T105016.0-00000104a-00000103a-90",
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
				"0000000100-20210728T105016.0-00000100a-00000099a-90",
				"0000000101-20210728T105016.0-00000101a-00000100a-90",
				"0000000102-20210728T105016.0-00000102a-00000101a-90",
				"0000000102-20210728T105016.0-00000102b-00000101a-90",
				"0000000103-20210728T105016.0-00000103b-00000102b-90",
				"0000000103-20210728T105016.0-00000103a-00000102a-90",
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
				"0000000106-20210728T105016.0-00000106a-00000104a-90",
				"0000000107-20210728T105016.0-00000107a-00000106a-90",
				"0000000108-20210728T105016.0-00000108b-00000107a-90",
				"0000000109-20210728T105016.0-00000109b-00000108b-90",
				"0000000110-20210728T105016.0-00000110b-00000109b-90",
				"0000000110-20210728T105016.0-00000110c-00000109b-90",
				"0000000111-20210728T105016.0-00000111c-00000110c-90",
				"0000000108-20210728T105016.0-00000108a-00000107a-90",
				"0000000109-20210728T105016.0-00000109a-00000108a-90",
				"0000000110-20210728T105016.0-00000110a-00000109a-90",
				"0000000111-20210728T105016.0-00000111a-00000110a-90",
				"0000000112-20210728T105016.0-00000112a-00000111a-90",
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
				"0000000107-20210728T105016.1-00000107a-00000106a-90",
				"0000000108-20210728T105016.2-00000108b-00000107a-90",
				"0000000109-20210728T105016.3-00000109b-00000108b-90",
				"0000000110-20210728T105016.4-00000110b-00000109b-90",
				"0000000110-20210728T105016.5-00000110c-00000109b-90",
				"0000000111-20210728T105016.6-00000111c-00000110c-90",
				"0000000108-20210728T105016.7-00000108a-00000107a-90",
				"0000000109-20210728T105016.8-00000109a-00000108a-90",
				"0000000110-20210728T105016.9-00000110a-00000109a-90",
				"0000000111-20210728T105016.10-00000111a-00000110a-90",
				"0000000112-20210728T105016.11-00000112a-00000111a-90",
				"0000000113-20210728T105016.12-00000113a-00000112a-90",
				"0000000114-20210728T105016.13-00000114a-00000113a-90",
				"0000000115-20210728T105016.14-00000115a-00000114a-90",
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
				"0000000115-20210728T105016.0-00000115a-00000114a-90",
				"0000000116-20210728T105016.0-00000116a-00000115a-90",
				"0000000117-20210728T105016.0-00000117a-00000116a-90",
				"0000000118-20210728T105016.0-00000118a-00000117a-90",
				"0000000120-20210728T105016.0-00000120a-00000118a-90",
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
			bundler := NewBundler(5, c.blockLimit)
			bundler.lastMergeOneBlockFile = &OneBlockFile{
				id: c.lastMergeBlockID,
			}
			for _, f := range c.files {
				bundler.AddOneBlockFile(MustTestNewOneBlockFile(f))
			}
			completed, highestBlockLimit := bundler.IsComplete()
			require.Equal(t, c.expectedCompleted, completed)
			require.Equal(t, c.expectedHighestBlockLimit, highestBlockLimit)
		})
	}

}

func MustTestNewOneBlockFile(fileName string) *OneBlockFile {
	blockNum, blockTime, blockID, previousBlockID, libNum, canonicalName, err := parseFilename(fileName)
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
		libNum:     libNum,
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
				MustTestNewOneBlockFile("0000000100-20210728T105016.01-00000100a-00000099a-90"),
				MustTestNewOneBlockFile("0000000101-20210728T105016.02-00000101a-00000100a-90"),
				MustTestNewOneBlockFile("0000000102-20210728T105016.03-00000102a-00000101a-90"),
				MustTestNewOneBlockFile("0000000102-20210728T105016.04-00000102b-00000101a-90"),
				MustTestNewOneBlockFile("0000000103-20210728T105016.05-00000103b-00000102b-90"),
				MustTestNewOneBlockFile("0000000103-20210728T105016.06-00000103a-00000102a-90"),
				MustTestNewOneBlockFile("0000000104-20210728T105016.07-00000104a-00000103a-90"),
				MustTestNewOneBlockFile("0000000106-20210728T105016.08-00000106a-00000104a-90"),
			},
			lastMergeBlockID:         "00000099a",
			blockLimit:               105,
			expectedIDs:              []string{"00000100a", "00000101a", "00000102a", "00000102b", "00000103b", "00000103a", "00000104a"},
			expectedLastMergeBlockID: "00000104a",
		},
		{
			name: "file 5",
			files: []*OneBlockFile{
				MustTestNewOneBlockFile("0000000106-20210728T105016.01-00000106a-00000104a-90"),
				MustTestNewOneBlockFile("0000000107-20210728T105016.02-00000107a-00000106a-90"),
				MustTestNewOneBlockFile("0000000108-20210728T105016.03-00000108b-00000107a-90"),
				MustTestNewOneBlockFile("0000000109-20210728T105016.04-00000109b-00000108b-90"),
				MustTestNewOneBlockFile("0000000110-20210728T105016.05-00000110b-00000109b-90"),
				MustTestNewOneBlockFile("0000000110-20210728T105016.06-00000110c-00000109b-90"),
				MustTestNewOneBlockFile("0000000111-20210728T105016.07-00000111c-00000110c-90"),
				MustTestNewOneBlockFile("0000000108-20210728T105016.08-00000108a-00000107a-90"),
				MustTestNewOneBlockFile("0000000109-20210728T105016.09-00000109a-00000108a-90"),
				MustTestNewOneBlockFile("0000000110-20210728T105016.10-00000110a-00000109a-90"),
				MustTestNewOneBlockFile("0000000111-20210728T105016.11-00000111a-00000110a-90"),
				MustTestNewOneBlockFile("0000000112-20210728T105016.12-00000112a-00000111a-90"),
			},
			lastMergeBlockID:         "00000104a",
			blockLimit:               110,
			expectedIDs:              []string{"00000106a", "00000107a", "00000108b", "00000109b", "00000108a", "00000109a"},
			expectedLastMergeBlockID: "00000109a",
		},
		{
			name: "file 10",
			files: []*OneBlockFile{
				MustTestNewMergedOneBlockFile("0000000107-20210728T105016.01-00000107a-00000106a-90"),
				MustTestNewMergedOneBlockFile("0000000108-20210728T105016.02-00000108b-00000107a-90"),
				MustTestNewMergedOneBlockFile("0000000109-20210728T105016.03-00000109b-00000108b-90"),
				MustTestNewOneBlockFile("0000000110-20210728T105016.04-00000110b-00000109b-90"),
				MustTestNewOneBlockFile("0000000110-20210728T105016.05-00000110c-00000109b-90"),
				MustTestNewOneBlockFile("0000000111-20210728T105016.06-00000111c-00000110c-90"),
				MustTestNewMergedOneBlockFile("0000000108-20210728T105016.07-00000108a-00000107a-90"),
				MustTestNewMergedOneBlockFile("0000000109-20210728T105016.08-00000109a-00000108a-90"),
				MustTestNewOneBlockFile("0000000110-20210728T105016.09-00000110a-00000109a-90"),
				MustTestNewOneBlockFile("0000000111-20210728T105016.10-00000111a-00000110a-90"),
				MustTestNewOneBlockFile("0000000112-20210728T105016.11-00000112a-00000111a-90"),
				MustTestNewOneBlockFile("0000000113-20210728T105016.12-00000113a-00000112a-90"),
				MustTestNewOneBlockFile("0000000114-20210728T105016.13-00000114a-00000113a-90"),
				MustTestNewOneBlockFile("0000000115-20210728T105016.14-00000115a-00000114a-90"),
			},
			lastMergeBlockID:         "00000109a",
			blockLimit:               115,
			expectedIDs:              []string{"00000110b", "00000110c", "00000111c", "00000110a", "00000111a", "00000112a", "00000113a", "00000114a"},
			expectedLastMergeBlockID: "00000114a",
		},
		{
			name: "file 15",
			files: []*OneBlockFile{
				MustTestNewOneBlockFile("0000000115-20210728T105016.0-00000115a-00000114a-90"),
				MustTestNewOneBlockFile("0000000116-20210728T105016.0-00000116a-00000115a-90"),
				MustTestNewOneBlockFile("0000000117-20210728T105016.0-00000117a-00000116a-90"),
				MustTestNewOneBlockFile("0000000118-20210728T105016.0-00000118a-00000117a-90"),
				MustTestNewOneBlockFile("0000000120-20210728T105016.0-00000120a-00000118a-90"),
			},
			lastMergeBlockID:         "00000114a",
			blockLimit:               120,
			expectedIDs:              []string{"00000115a", "00000116a", "00000117a", "00000118a"},
			expectedLastMergeBlockID: "00000118a",
		},
		{
			name: "file with holes",
			files: []*OneBlockFile{
				MustTestNewOneBlockFile("0000000100-20210728T105016.0-00000100a-00000099a-90"),

				MustTestNewOneBlockFile("0000000115-20210728T105016.0-00000115a-00000114a-90"),
				MustTestNewOneBlockFile("0000000116-20210728T105016.0-00000116a-00000115a-90"),

				MustTestNewOneBlockFile("0000000117-20210728T105016.0-00000117b-00000116b-90"),
				MustTestNewOneBlockFile("0000000118-20210728T105016.0-00000118b-00000117b-90"),

				MustTestNewOneBlockFile("0000000117-20210728T105016.1-00000117a-00000116a-90"),
				MustTestNewOneBlockFile("0000000118-20210728T105016.1-00000118a-00000117a-90"),
				MustTestNewOneBlockFile("0000000120-20210728T105016.0-00000120a-00000118a-90"),
			},
			lastMergeBlockID:         "00000114a",
			blockLimit:               120,
			expectedIDs:              []string{"00000100a", "00000115a", "00000116a", "00000117b", "00000118b", "00000117a", "00000118a"},
			expectedLastMergeBlockID: "00000118a",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			bundler := NewBundler(5, c.blockLimit)
			bundler.lastMergeOneBlockFile = &OneBlockFile{id: c.lastMergeBlockID}
			for _, f := range c.files {
				bundler.AddOneBlockFile(f)
			}
			completed, highestBlockLimit := bundler.IsComplete()
			require.True(t, completed)
			mergeableFiles := bundler.ToBundle(highestBlockLimit)
			bundler.Commit(highestBlockLimit)

			ids := toIDs(mergeableFiles)
			require.Equal(t, c.expectedIDs, ids)
			require.Equal(t, c.expectedLastMergeBlockID, bundler.lastMergeOneBlockFile.id)
		})
	}
}
func toIDs(oneBlockFileList []*OneBlockFile) (ids []string) {
	for _, file := range oneBlockFileList {
		ids = append(ids, file.id)
	}
	return ids
}

func TestBundler_Complicated(t *testing.T) {

	files := []*OneBlockFile{
		MustTestNewOneBlockFile("0000000100-20210728T105016.01-00000100a-00000099a-90"),
		MustTestNewOneBlockFile("0000000101-20210728T105016.02-00000101a-00000100a-90"),
		MustTestNewOneBlockFile("0000000102-20210728T105016.03-00000102a-00000101a-90"),
		MustTestNewOneBlockFile("0000000102-20210728T105016.04-00000102b-00000101a-90"),
		MustTestNewOneBlockFile("0000000103-20210728T105016.05-00000103b-00000102b-90"),
		MustTestNewOneBlockFile("0000000103-20210728T105016.06-00000103a-00000102a-90"),
		MustTestNewOneBlockFile("0000000104-20210728T105016.07-00000104a-00000103a-90"),
		MustTestNewOneBlockFile("0000000106-20210728T105016.08-00000106a-00000104a-90"),
		MustTestNewOneBlockFile("0000000107-20210728T105016.09-00000107a-00000106a-90"),
		MustTestNewOneBlockFile("0000000108-20210728T105016.10-00000108b-00000107a-90"),
		MustTestNewOneBlockFile("0000000109-20210728T105016.11-00000109b-00000108b-90"),
		MustTestNewOneBlockFile("0000000110-20210728T105016.12-00000110b-00000109b-90"),
		MustTestNewOneBlockFile("0000000110-20210728T105016.13-00000110c-00000109b-90"),
		MustTestNewOneBlockFile("0000000111-20210728T105016.14-00000111c-00000110c-90"),
		MustTestNewOneBlockFile("0000000108-20210728T105016.15-00000108a-00000107a-90"),
		MustTestNewOneBlockFile("0000000109-20210728T105016.16-00000109a-00000108a-90"),
		MustTestNewOneBlockFile("0000000110-20210728T105016.17-00000110a-00000109a-90"),
		MustTestNewOneBlockFile("0000000111-20210728T105016.18-00000111a-00000110a-90"),
		MustTestNewOneBlockFile("0000000112-20210728T105016.19-00000112a-00000111a-90"),
		MustTestNewOneBlockFile("0000000113-20210728T105016.20-00000113a-00000112a-90"),
		MustTestNewOneBlockFile("0000000114-20210728T105016.21-00000114a-00000113a-90"),
		MustTestNewOneBlockFile("0000000115-20210728T105016.22-00000115a-00000114a-90"),
		MustTestNewOneBlockFile("0000000116-20210728T105016.23-00000116a-00000115a-90"),
		MustTestNewOneBlockFile("0000000117-20210728T105016.24-00000117a-00000116a-90"),
		MustTestNewOneBlockFile("0000000118-20210728T105016.25-00000118a-00000117a-90"),
		MustTestNewOneBlockFile("0000000120-20210728T105016.26-00000120a-00000118a-90"),
	}

	bundler := NewBundler(5, 105)
	bundler.lastMergeOneBlockFile = &OneBlockFile{id: "00000099a"}
	for _, f := range files {
		bundler.AddOneBlockFile(f)
	}

	completed, highestBlockLimit := bundler.IsComplete()
	require.True(t, completed)
	mergeableFiles := bundler.ToBundle(highestBlockLimit)
	bundler.Commit(highestBlockLimit)

	ids := toIDs(mergeableFiles)
	require.Equal(t, []string{"00000100a", "00000101a", "00000102a", "00000102b", "00000103b", "00000103a", "00000104a"}, ids)

	completed, highestBlockLimit = bundler.IsComplete()
	require.True(t, completed)
	mergeableFiles = bundler.ToBundle(highestBlockLimit)
	bundler.Commit(highestBlockLimit)
	ids = toIDs(mergeableFiles)
	require.Equal(t, []string{"00000106a", "00000107a", "00000108b", "00000109b", "00000108a", "00000109a"}, ids)

	completed, highestBlockLimit = bundler.IsComplete()
	require.True(t, completed)
	mergeableFiles = bundler.ToBundle(highestBlockLimit)
	bundler.Commit(highestBlockLimit)

	ids = toIDs(mergeableFiles)
	require.Equal(t, []string{"00000110b", "00000110c", "00000111c", "00000110a", "00000111a", "00000112a", "00000113a", "00000114a"}, ids)

	completed, highestBlockLimit = bundler.IsComplete()
	require.True(t, completed)
	mergeableFiles = bundler.ToBundle(highestBlockLimit)
	bundler.Commit(highestBlockLimit)

	ids = toIDs(mergeableFiles)
	require.Equal(t, []string{"00000115a", "00000116a", "00000117a", "00000118a"}, ids)
}

func TestBundler_BackToTheFuture(t *testing.T) {
	// load forkdb with some history ...
	files := []*OneBlockFile{
		MustTestNewMergedOneBlockFile("0000000094-20210728T105016.01-00000094a-00000093a-90"),
		MustTestNewMergedOneBlockFile("0000000095-20210728T105016.01-00000095a-00000094a-90"),
		MustTestNewMergedOneBlockFile("0000000096-20210728T105016.01-00000096a-00000095a-90"),
		MustTestNewMergedOneBlockFile("0000000097-20210728T105016.01-00000097a-00000096a-90"),
		MustTestNewMergedOneBlockFile("0000000098-20210728T105016.01-00000098a-00000097a-90"),
		MustTestNewMergedOneBlockFile("0000000099-20210728T105016.01-00000099a-00000098a-90"),

		MustTestNewOneBlockFile("0000000100-20210728T105016.01-00000100a-00000099a-90"),
		MustTestNewOneBlockFile("0000000101-20210728T105016.02-00000101a-00000100a-90"),
		MustTestNewOneBlockFile("0000000102-20210728T105016.03-00000102a-00000101a-90"),
		MustTestNewOneBlockFile("0000000103-20210728T105016.06-00000103a-00000102a-90"),
		MustTestNewOneBlockFile("0000000104-20210728T105016.07-00000104a-00000103a-90"),
		MustTestNewOneBlockFile("0000000106-20210728T105016.08-00000106a-00000104a-90"),
	}

	bundler := NewBundler(5, 105)
	bundler.lastMergeOneBlockFile = &OneBlockFile{id: "00000099a"}
	for _, f := range files {
		bundler.AddOneBlockFile(f)
	}

	// Let's merge a first back of block from 100 to 104
	completed, highestBlockLimit := bundler.IsComplete()
	require.True(t, completed)
	mergeableFiles := bundler.ToBundle(highestBlockLimit)
	ids := toIDs(mergeableFiles)
	require.Equal(t, []string{"00000100a", "00000101a", "00000102a", "00000103a", "00000104a"}, ids)
	bundler.Commit(highestBlockLimit)

	// Add a very old file
	bundler.AddOneBlockFile(MustTestNewOneBlockFile("000000095-20210728T105015.01-00000095b-00000094a-90"))

	//that new file should not trigger a merge
	completed, highestBlockLimit = bundler.IsComplete()
	require.False(t, completed)

	// Add missing file for this back in time fork
	bundler.AddOneBlockFile(MustTestNewOneBlockFile("000000096-20210728T105015.02-00000096b-00000095b-90"))
	bundler.AddOneBlockFile(MustTestNewOneBlockFile("000000097-20210728T105015.03-00000097b-00000096b-90"))
	bundler.AddOneBlockFile(MustTestNewOneBlockFile("000000098-20210728T105015.04-00000098b-00000097b-90"))
	bundler.AddOneBlockFile(MustTestNewOneBlockFile("000000099-20210728T105015.05-00000099b-00000098b-90"))
	bundler.AddOneBlockFile(MustTestNewOneBlockFile("000000100-20210728T105015.06-00000100b-00000099b-90"))
	bundler.AddOneBlockFile(MustTestNewOneBlockFile("000000101-20210728T105015.07-00000101b-00000100b-90"))
	bundler.AddOneBlockFile(MustTestNewOneBlockFile("000000102-20210728T105015.08-00000102b-00000101b-90"))
	bundler.AddOneBlockFile(MustTestNewOneBlockFile("000000103-20210728T105015.09-00000103b-00000102b-90"))
	bundler.AddOneBlockFile(MustTestNewOneBlockFile("000000104-20210728T105015.10-00000104b-00000103b-90"))
	bundler.AddOneBlockFile(MustTestNewOneBlockFile("000000105-20210728T105015.11-00000105b-00000104b-90"))
	bundler.AddOneBlockFile(MustTestNewOneBlockFile("000000106-20210728T105015.12-00000106b-00000105b-90"))
	bundler.AddOneBlockFile(MustTestNewOneBlockFile("000000107-20210728T105015.12-00000107b-00000106b-90"))
	bundler.AddOneBlockFile(MustTestNewOneBlockFile("000000108-20210728T105015.12-00000108b-00000107b-90"))
	bundler.AddOneBlockFile(MustTestNewOneBlockFile("000000109-20210728T105015.12-00000109b-00000108b-90"))

	completed, highestBlockLimit = bundler.IsComplete()
	//should not trigger merge yet
	require.False(t, completed)

	bundler.AddOneBlockFile(MustTestNewOneBlockFile("000000110-20210728T105015.12-00000110b-00000109b-90"))

	completed, highestBlockLimit = bundler.IsComplete()
	//here we go!
	require.True(t, completed)

	mergeableFiles = bundler.ToBundle(highestBlockLimit)
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

func TestBundler_Purge(t *testing.T) {

	cases := []struct {
		name                      string
		files                     []*OneBlockFile
		lastMergerBlock           *OneBlockFile
		expectedPurgedFileCount   int
		expectedLongestFirstBlock string
		expectedLibID             string
	}{
		{
			name: "Sunny path",
			files: []*OneBlockFile{
				MustTestNewOneBlockFile("0000000100-20210728T105016.01-00000100a-00000099a-90"),
				MustTestNewOneBlockFile("0000000101-20210728T105016.02-00000101a-00000100a-100"),
				MustTestNewOneBlockFile("0000000102-20210728T105016.03-00000102a-00000101a-100"),
				MustTestNewOneBlockFile("0000000103-20210728T105016.06-00000103a-00000102a-100"),
				MustTestNewOneBlockFile("0000000104-20210728T105016.07-00000104a-00000103a-101"),
				MustTestNewOneBlockFile("0000000106-20210728T105016.08-00000106a-00000104a-101"),
			},
			lastMergerBlock:           MustTestNewOneBlockFile("0000000104-20210728T105016.07-00000104a-00000103a-101"),
			expectedLongestFirstBlock: "00000101a",
			expectedPurgedFileCount:   1,
			expectedLibID:             "00000101a",
		},
		{
			name: "Sunny path with fork",
			files: []*OneBlockFile{
				MustTestNewOneBlockFile("0000000100-20210728T105016.01-00000100a-00000099a-90"),
				MustTestNewOneBlockFile("0000000101-20210728T105016.02-00000101a-00000100a-100"),
				MustTestNewOneBlockFile("0000000102-20210728T105016.03-00000102a-00000101a-100"),
				MustTestNewOneBlockFile("0000000102-20210728T105016.03-00000102b-00000101a-100"),
				MustTestNewOneBlockFile("0000000103-20210728T105016.06-00000103b-00000102a-100"),
				MustTestNewOneBlockFile("0000000103-20210728T105016.06-00000103a-00000102a-100"),
				MustTestNewOneBlockFile("0000000104-20210728T105016.07-00000104a-00000103a-101"),
				MustTestNewOneBlockFile("0000000106-20210728T105016.08-00000106a-00000104a-101"),
			},
			lastMergerBlock:           MustTestNewOneBlockFile("0000000104-20210728T105016.07-00000104a-00000103a-101"),
			expectedLongestFirstBlock: "00000101a",
			expectedPurgedFileCount:   1,
			expectedLibID:             "00000101a",
		},
		{
			name: "Purger fork",
			files: []*OneBlockFile{
				MustTestNewOneBlockFile("0000000100-20210728T105016.01-00000100a-00000099a-90"),
				MustTestNewOneBlockFile("0000000101-20210728T105016.02-00000101a-00000100a-100"),
				MustTestNewOneBlockFile("0000000102-20210728T105016.03-00000102a-00000101a-100"),
				MustTestNewOneBlockFile("0000000102-20210728T105016.03-00000102b-00000101a-100"),
				MustTestNewOneBlockFile("0000000103-20210728T105016.06-00000103b-00000102a-100"),
				MustTestNewOneBlockFile("0000000103-20210728T105016.06-00000103a-00000102a-100"),
				MustTestNewOneBlockFile("0000000104-20210728T105016.07-00000104a-00000103a-103"),
				MustTestNewOneBlockFile("0000000106-20210728T105016.08-00000106a-00000104a-101"),
			},
			lastMergerBlock:           MustTestNewOneBlockFile("0000000104-20210728T105016.07-00000104a-00000103a-103"),
			expectedLongestFirstBlock: "00000103a",
			expectedPurgedFileCount:   4,
			expectedLibID:             "00000103a",
		},
		{
			name: "Purge nothing never merged anything",
			files: []*OneBlockFile{
				MustTestNewOneBlockFile("0000000100-20210728T105016.01-00000100a-00000099a-90"),
				MustTestNewOneBlockFile("0000000101-20210728T105016.02-00000101a-00000100a-90"),
			},
			lastMergerBlock:           nil,
			expectedLongestFirstBlock: "00000100a",
			expectedPurgedFileCount:   0,
			expectedLibID:             "",
		},
		{
			name: "Purge nothing",
			files: []*OneBlockFile{
				MustTestNewOneBlockFile("0000000100-20210728T105016.01-00000100a-00000099a-90"),
				MustTestNewOneBlockFile("0000000101-20210728T105016.02-00000101a-00000100a-90"),
			},
			lastMergerBlock:           MustTestNewOneBlockFile("0000000101-20210728T105016.02-00000101a-00000100a-90"),
			expectedLongestFirstBlock: "00000100a",
			expectedPurgedFileCount:   0,
			expectedLibID:             "",
		},
		{
			name: "Purge multiple root",
			files: []*OneBlockFile{
				MustTestNewOneBlockFile("0000000100-20210728T105016.01-00000100b-00000099b-90"),
				MustTestNewOneBlockFile("0000000101-20210728T105016.02-00000101b-00000100b-100"),
				MustTestNewOneBlockFile("0000000102-20210728T105016.03-00000102b-00000101b-100"),

				MustTestNewOneBlockFile("0000000100-20210728T105016.01-00000100a-00000099a-90"),
				MustTestNewOneBlockFile("0000000101-20210728T105016.02-00000101a-00000100a-100"),
				MustTestNewOneBlockFile("0000000102-20210728T105016.03-00000102a-00000101a-100"),
				MustTestNewOneBlockFile("0000000103-20210728T105016.06-00000103a-00000102a-100"),
				MustTestNewOneBlockFile("0000000104-20210728T105016.07-00000104a-00000103a-103"),
				MustTestNewOneBlockFile("0000000106-20210728T105016.08-00000106a-00000104a-103"),
			},
			lastMergerBlock:           MustTestNewOneBlockFile("0000000104-20210728T105016.07-00000104a-00000103a-103"),
			expectedLongestFirstBlock: "00000103a",
			expectedPurgedFileCount:   6,
			expectedLibID:             "00000103a",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			bundler := NewBundler(5, 105)
			for _, f := range c.files {
				bundler.AddOneBlockFile(f)
			}

			bundler.lastMergeOneBlockFile = c.lastMergerBlock

			bundler.Purge(func(purgedOneBlockFiles []*OneBlockFile) {
				require.Equal(t, c.expectedPurgedFileCount, len(purgedOneBlockFiles))
			})

			require.Equal(t, c.expectedLibID, bundler.db.LIBID())

			_, err := bundler.db.Roots()
			if c.expectedLongestFirstBlock == "" {
				require.Errorf(t, err, "no link")
				return
			}
			require.NoError(t, err)

			longest := bundler.LongestChain()
			require.Equal(t, c.expectedLongestFirstBlock, longest[0])
		})
	}
}

func TestBundler_Boostrap(t *testing.T) {
	mergeFiles := map[uint64][]*OneBlockFile{
		95: {
			MustTestNewOneBlockFile("0000000095-20210728T105016.07-00000095a-00000094a-90"),
			MustTestNewOneBlockFile("0000000096-20210728T105016.07-00000096a-00000095a-90"),
			MustTestNewOneBlockFile("0000000097-20210728T105016.07-00000097a-00000096a-95"),
			MustTestNewOneBlockFile("0000000098-20210728T105016.07-00000098a-00000097a-95"),
			MustTestNewOneBlockFile("0000000098-20210728T105016.07-00000098b-00000097a-95"),
			MustTestNewOneBlockFile("0000000099-20210728T105016.07-00000099a-00000098a-95"),
			MustTestNewOneBlockFile("0000000099-20210728T105016.07-00000099b-00000098b-95"),
		},
		100: {
			MustTestNewOneBlockFile("0000000100-20210728T105016.01-00000100a-00000099a-95"),
			MustTestNewOneBlockFile("0000000100-20210728T105016.01-00000100b-00000099b-95"),
			MustTestNewOneBlockFile("0000000101-20210728T105016.02-00000101a-00000100a-98"),
			MustTestNewOneBlockFile("0000000102-20210728T105016.03-00000102a-00000101a-98"),
			MustTestNewOneBlockFile("0000000103-20210728T105016.06-00000103a-00000102a-98"),
			MustTestNewOneBlockFile("0000000104-20210728T105016.07-00000104a-00000103a-98"),
		},
		105: {
			MustTestNewOneBlockFile("0000000106-20210728T105016.08-00000106a-00000104a-101"),
			MustTestNewOneBlockFile("0000000107-20210728T105016.09-00000107a-00000106a-101"),
			MustTestNewOneBlockFile("0000000108-20210728T105016.15-00000108a-00000107a-101"),
			MustTestNewOneBlockFile("0000000109-20210728T105016.16-00000109a-00000108a-106"),
		},
		110: {
			MustTestNewOneBlockFile("0000000110-20210728T105016.17-00000110a-00000109a-108"),
			MustTestNewOneBlockFile("0000000111-20210728T105016.18-00000111a-00000110a-108"),
			MustTestNewOneBlockFile("0000000112-20210728T105016.19-00000112a-00000111a-108"),
			MustTestNewOneBlockFile("0000000113-20210728T105016.20-00000113a-00000112a-111"),
			MustTestNewOneBlockFile("0000000114-20210728T105016.21-00000114a-00000113a-113"),
		},
	}

	testCases := []struct {
		name                            string
		firstExclusiveHighestBlockLimit uint64
		expectedMergeFilesRead          []int
		expectedFirstBlockNum           uint64
		expectedErr                     bool
	}{
		{
			name:                            "Sunny path",
			firstExclusiveHighestBlockLimit: 115,
			expectedMergeFilesRead:          []int{110},
			expectedFirstBlockNum:           113,
		},
		{
			name:                            "First and last from single file",
			firstExclusiveHighestBlockLimit: 110,
			expectedMergeFilesRead:          []int{105},
			expectedFirstBlockNum:           106,
		},
		{
			name:                            "Find lib over 2 files",
			firstExclusiveHighestBlockLimit: 105,
			expectedMergeFilesRead:          []int{100, 95},
			expectedFirstBlockNum:           98,
		},
	}

	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			bundler := NewBundler(5, c.firstExclusiveHighestBlockLimit)
			var mergeFileReads []int
			err := bundler.Boostrap(func(lowBlockNum uint64) ([]*OneBlockFile, error) {
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
	oneBlockFiles := []*OneBlockFile{
		MustTestNewOneBlockFile("0000000100-20210728T105016.01-00000100a-00000099a-90"),
		MustTestNewOneBlockFile("0000000101-20210728T105016.02-00000101a-00000100a-90"),
		MustTestNewOneBlockFile("0000000102-20210728T105016.03-00000102a-00000101a-90"),
		MustTestNewOneBlockFile("0000000103-20210728T105016.06-00000103a-00000102a-90"),
		MustTestNewOneBlockFile("0000000104-20210728T105016.07-00000104a-00000103a-90"),
		MustTestNewOneBlockFile("0000000106-20210728T105016.08-00000106a-00000104a-90"),
	}

	oneBlockFilesTwoRoots := []*OneBlockFile{
		MustTestNewOneBlockFile("000000095-20210728T105016.01-00000095b-00000094a-90"),
		MustTestNewOneBlockFile("0000000100-20210728T105016.01-00000100a-00000099a-90"),
		MustTestNewOneBlockFile("0000000101-20210728T105016.02-00000101a-00000100a-90"),
		MustTestNewOneBlockFile("0000000102-20210728T105016.03-00000102a-00000101a-90"),
		MustTestNewOneBlockFile("0000000103-20210728T105016.06-00000103a-00000102a-90"),
		MustTestNewOneBlockFile("0000000104-20210728T105016.07-00000104a-00000103a-90"),
		MustTestNewOneBlockFile("0000000106-20210728T105016.08-00000106a-00000104a-90"),
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
			files:          oneBlockFiles,
			blockNum:       102,
			expectedResult: false,
		},
		{
			name:           "in the future",
			files:          oneBlockFiles,
			blockNum:       200,
			expectedResult: false,
		},
		{
			name:           "at first block",
			files:          oneBlockFiles,
			blockNum:       100,
			expectedResult: false,
		},
		{
			name:           "before first block",
			files:          oneBlockFiles,
			blockNum:       99,
			expectedResult: true,
		},
		{
			name:           "too old",
			files:          oneBlockFiles,
			blockNum:       5,
			expectedResult: true,
		},
		{
			name:           "in the middle 2 roots",
			files:          oneBlockFilesTwoRoots,
			blockNum:       102,
			expectedResult: false,
		},
		{
			name:           "in the future 2 roots",
			files:          oneBlockFilesTwoRoots,
			blockNum:       200,
			expectedResult: false,
		},
		{
			name:           "at first block 2 roots",
			files:          oneBlockFilesTwoRoots,
			blockNum:       95,
			expectedResult: false,
		},
		{
			name:           "before first block 2 roots",
			files:          oneBlockFilesTwoRoots,
			blockNum:       94,
			expectedResult: true,
		},
		{
			name:           "too old 2 roots",
			files:          oneBlockFilesTwoRoots,
			blockNum:       5,
			expectedResult: true,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			bundler := NewBundler(5, 105)
			for _, f := range c.files {
				bundler.AddOneBlockFile(f)
			}
			tooOld := bundler.IsBlockTooOld(c.blockNum)
			require.Equal(t, c.expectedResult, tooOld)

		})
	}
}
