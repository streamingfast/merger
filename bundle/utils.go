package bundle

import "sort"

func ToSortedIDs(oneBlockFileList []*OneBlockFile) (ids []string) {
	sort.Slice(oneBlockFileList, func(i, j int) bool {
		if oneBlockFileList[i].BlockTime.Equal(oneBlockFileList[j].BlockTime) {
			return oneBlockFileList[i].Num < oneBlockFileList[j].Num
		}
		return oneBlockFileList[i].BlockTime.Before(oneBlockFileList[j].BlockTime)
	})
	return ToIDs(oneBlockFileList)
}

func ToIDs(oneBlockFileList []*OneBlockFile) (ids []string) {
	for _, file := range oneBlockFileList {
		ids = append(ids, file.ID)
	}
	return ids
}
