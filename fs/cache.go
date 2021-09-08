// Copyright (c) 2021 Andrew Aukerman.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package minfs

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// File implements both Node and Handle for the hello file.
type CacheItem struct {
	Path    string
	Size    float64
	ModTime time.Time
}

func DirSize(path string) ([]CacheItem, float64, error) {
	var totalSize float64
	var items []CacheItem

	err := filepath.Walk(path, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && strings.Contains(info.Name(), ".fcache") {
			sizeGB := float64(info.Size()) / math.Pow(1024.0, 3.0)

			f := CacheItem{Path: path, Size: sizeGB, ModTime: info.ModTime()}
			totalSize += sizeGB
			items = append(items, f)
		}
		return err
	})

	sort.Slice(items[:], func(i, j int) bool {
		return items[i].ModTime.Before(items[j].ModTime)
	})

	return items, totalSize, err
}

func (mfs *MinFS) DeleteUntilQuota(items []CacheItem, quota float64) {
	for _, item := range items {
		fmt.Println(item)
		// Lock the cache resource until we are done deleting
		unlock := mfs.km.Lock(item.Path)

		// Need to lock the map down for a second..
		used := false
		mfs.m.Lock()
		for _, cachePath := range mfs.openfds {
			used = used || (cachePath == item.Path)
		}
		mfs.m.Unlock()
		fmt.Println("Used:", used)

		if !used {
			quota -= item.Size
			os.Remove(item.Path)
		}

		if quota < 0 {
			break
		}

		unlock()
	}

}

func (mfs *MinFS) MonitorCache() {
	fmt.Println("Starting cache monitor!")
	defer mfs.m.Unlock()

	MAX_SIZE := float64(0)

	for {
		select {

		case <-time.After(1 * time.Second):
			items, size, err := DirSize(mfs.config.cache)
			if err != nil {
				fmt.Println("Error getting cache director...")
			} else if size <= MAX_SIZE {
				fmt.Println("Cache OK: Cache files:", len(items), "size:", size, "GB")
				fmt.Println(items)

			} else {
				mfs.m.Lock()
				fmt.Println("Cache CLEANUP: Cache files:", len(items), "size:", size, "GB", "open FDs:", len(mfs.openfds))
				mfs.m.Unlock()

				fmt.Println(mfs.openfds)
				mfs.DeleteUntilQuota(items, size-MAX_SIZE)

			}

		}
	}
}
