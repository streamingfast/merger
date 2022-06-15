package forkresolver

import (
	"fmt"
	"testing"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/merger/bundle"
	"github.com/test-go/testify/assert"
	"github.com/test-go/testify/require"
)

// FIXME: think of solving the irreversible steps too from cursor

func TestResolve(t *testing.T) {

	cases := []struct {
		name              string
		oneBlockFiles     []string
		mergedBlocksFiles map[uint64]map[string]*bundle.OneBlockFile
		block             bstream.BlockRef
		lib               bstream.BlockRef
		expectUndoBlocks  []bstream.BlockRef
		expectNextBlock   uint64
		expectError       error
	}{
		{
			name: "on correct block",
			oneBlockFiles: []string{
				"0000000102-20210728T105016.0-0000102b-0000101a-100-suffix",
				"0000000103-20210728T105016.0-0000103b-0000102b-100-suffix",
			},
			mergedBlocksFiles: map[uint64]map[string]*bundle.OneBlockFile{
				100: parseFilenames([]string{
					"0000000100-20210728T105016.0-0000100a-0000099a-90-suffix",
					"0000000101-20210728T105016.0-0000101a-0000100a-100-suffix",
					"0000000102-20210728T105016.0-0000102a-0000101a-100-suffix",
					"0000000103-20210728T105016.0-0000103a-0000102a-100-suffix",
					"0000000104-20210728T105016.0-0000104a-0000103a-100-suffix",
				}, 199),
			},
			block:            bstream.NewBlockRef("deadbeefdeadbeef00000103a", 103),
			lib:              bstream.NewBlockRef("d00db00fd00db00f00000100a", 100),
			expectUndoBlocks: nil,
			expectNextBlock:  103,
		},
		{
			name: "undo two blocks",
			oneBlockFiles: []string{
				"0000000102-20210728T105016.0-0000102b-0000101a-100-suffix",
				"0000000103-20210728T105016.0-0000103b-0000102b-100-suffix",
			},
			mergedBlocksFiles: map[uint64]map[string]*bundle.OneBlockFile{
				100: parseFilenames([]string{
					"0000000100-20210728T105016.0-0000100a-0000099a-90-suffix",
					"0000000101-20210728T105016.0-0000101a-0000100a-100-suffix",
					"0000000102-20210728T105016.0-0000102a-0000101a-100-suffix",
					"0000000103-20210728T105016.0-0000103a-0000102a-100-suffix",
					"0000000104-20210728T105016.0-0000104a-0000103a-100-suffix",
				}, 199),
			},
			block:            bstream.NewBlockRef("deadbeefdeadbeef00000103b", 103),
			lib:              bstream.NewBlockRef("d00db00fd00db00f00000100a", 100),
			expectUndoBlocks: []bstream.BlockRef{bstream.NewBlockRef("0000103b", 103), bstream.NewBlockRef("0000102b", 102)},
			expectNextBlock:  101,
		},
		{
			name: "undo passed merged boundary",
			oneBlockFiles: []string{
				"0000000102-20210728T105016.0-0000102b-0000101a-100-suffix",
				"0000000103-20210728T105016.0-0000103b-0000102b-100-suffix",
				"0000000106-20210728T105016.0-0000106b-0000104a-100-suffix",
				"0000000201-20210728T115016.0-0000201b-0000106b-104-suffix",
				"0000000202-20210728T115016.0-0000202b-0000201b-104-suffix",
			},
			mergedBlocksFiles: map[uint64]map[string]*bundle.OneBlockFile{
				100: parseFilenames([]string{
					"0000000100-20210728T105016.0-0000100a-0000099a-90-suffix",
					"0000000101-20210728T105016.0-0000101a-0000100a-100-suffix",
					"0000000102-20210728T105016.0-0000102a-0000101a-100-suffix",
					"0000000103-20210728T105016.0-0000103a-0000102a-100-suffix",
					"0000000104-20210728T105016.0-0000104a-0000103a-100-suffix",
					"0000000106-20210728T105016.0-0000106a-0000104a-100-suffix",
					"0000000109-20210728T105016.0-0000109a-0000106a-104-suffix",
				}, 199),
				200: parseFilenames([]string{
					"0000000200-20210728T105116.0-0000200a-0000106a-104-suffix",
					"0000000201-20210728T105116.0-0000201a-0000200a-104-suffix",
					"0000000202-20210728T105116.0-0000202a-0000201a-104-suffix",
					"0000000203-20210728T105116.0-0000203a-0000202a-104-suffix",
					"0000000204-20210728T105116.0-0000204a-0000203a-104-suffix",
					"0000000206-20210728T105116.0-0000206a-0000204a-104-suffix",
				}, 299),
			},
			block: bstream.NewBlockRef("deadbeefdeadbeef00000202b", 202),
			lib:   bstream.NewBlockRef("d00db00fd00db00f00000104a", 104),
			expectUndoBlocks: []bstream.BlockRef{
				bstream.NewBlockRef("0000202b", 202),
				bstream.NewBlockRef("0000201b", 201),
				bstream.NewBlockRef("0000106b", 106),
			},
			expectNextBlock: 104,
		},
		{
			name: "missing oneBlockFiles",
			oneBlockFiles: []string{
				"0000000102-20210728T105016.0-0000102b-0000101a-100-suffix",
				"0000000103-20210728T105016.0-0000103b-0000102b-100-suffix",
			},
			mergedBlocksFiles: map[uint64]map[string]*bundle.OneBlockFile{
				100: parseFilenames([]string{
					"0000000100-20210728T105016.0-0000100a-0000099a-90-suffix",
					"0000000101-20210728T105016.0-0000101a-0000100a-100-suffix",
					"0000000102-20210728T105016.0-0000102a-0000101a-100-suffix",
					"0000000103-20210728T105016.0-0000103a-0000102a-100-suffix",
					"0000000104-20210728T105016.0-0000104a-0000103a-100-suffix",
				}, 199),
			},
			block:       bstream.NewBlockRef("deadbeefdeadbeef00000104b", 104),
			lib:         bstream.NewBlockRef("d00db00fd00db00f00000100a", 100),
			expectError: fmt.Errorf("cannot resolve block #104 (deadbeefdeadbeef00000104b): no oneBlockFile or merged block found with ID 0000104b"),
		},
		{
			name: "missing mergedBlockFile",
			oneBlockFiles: []string{
				"0000000102-20210728T105016.0-0000102b-0000101a-100-suffix",
				"0000000103-20210728T105016.0-0000103b-0000102b-100-suffix",
				"0000000203-20210728T105016.0-0000203b-0000103b-100-suffix",
			},
			mergedBlocksFiles: map[uint64]map[string]*bundle.OneBlockFile{
				200: parseFilenames([]string{
					"0000000203-20210728T105016.0-0000203a-0000103a-100-suffix",
				}, 299),
			},
			block:       bstream.NewBlockRef("deadbeefdeadbeef00000203b", 203),
			lib:         bstream.NewBlockRef("d00db00fd00db00f00000100a", 100),
			expectError: fmt.Errorf("cannot resolve block #203 (deadbeefdeadbeef00000203b) (cannot load previous bundle (100): file not found)"),
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {

			var getBlocksFromMerged = func(base uint64) (map[string]*bundle.OneBlockFile, error) {
				if c.mergedBlocksFiles[base] == nil {
					return nil, fmt.Errorf("file not found")
				}
				return c.mergedBlocksFiles[base], nil
			}
			var getOneBlocks = func(upto uint64) map[string]*bundle.OneBlockFile {
				return parseFilenames(c.oneBlockFiles, upto)
			}

			res := &resolver{
				mergedBlockFilesGetter: getBlocksFromMerged,
				oneBlockFilesGetter:    getOneBlocks,
			}

			undos, next, err := res.resolve(c.block, c.lib)
			if c.expectError != nil {
				require.Error(t, err)
				assert.Equal(t, c.expectError.Error(), err.Error())
				return
			}
			require.NoError(t, err)
			assert.Equal(t, c.expectUndoBlocks, undos)
			assert.Equal(t, c.expectNextBlock, next)
		})
	}

}
