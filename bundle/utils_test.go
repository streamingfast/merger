package bundle

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestUtils_ToSortedIDs(t *testing.T) {
	srcOneBlockFiles := []*OneBlockFile{
		MustNewOneBlockFile("0000000001-20210728T105016.01-00000001a-00000000a-0-suffix"),
		MustNewOneBlockFile("0000000002-20210728T105016.02-00000002a-00000001a-0-suffix"),
		MustNewOneBlockFile("0000000003-20210728T105016.03-00000003a-00000002a-0-suffix"),
		MustNewOneBlockFile("0000000004-20210728T105016.06-00000004a-00000003a-0-suffix"),
		MustNewOneBlockFile("0000000006-20210728T105016.08-00000006a-00000004a-0-suffix"),
	}

	// ensure two blocks have the same block time
	blockTime := srcOneBlockFiles[0].BlockTime
	srcOneBlockFiles[2].BlockTime = blockTime

	res := ToSortedIDs(srcOneBlockFiles)
	require.IsType(t, []string{}, res)
	require.Equal(t, len(res), len(srcOneBlockFiles))
}
