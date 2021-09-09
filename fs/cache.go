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
	"time"
)

// File implements both Node and Handle for the hello file.
type CacheItem struct {
	Path    string
	Size    float64
	ModTime time.Time
}

// Return cache items for cache directory
func DirSize(path string) ([]CacheItem, float64, error) {
	var totalSize float64
	var items []CacheItem

	err := filepath.Walk(path, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && filepath.Ext(path) == ".fcache" {
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

// Deletes cache items until size quota is satisified
func (mfs *MinFS) DeleteUntilQuota(items []CacheItem, quota float64) {
	for _, item := range items {
		// Lock the cache resource until we are done deleting
		unlock := mfs.km.Lock(item.Path)

		// Need to lock the map as we check..
		used := false
		mfs.m.Lock()

		// Search for open file handles that are using our cache resource
		for _, cachePath := range mfs.openfds {
			used = used || (cachePath == item.Path)
			if used {
				break // Hold the map lock for as short as possible
			}
		}
		mfs.m.Unlock()

		// Since we've locked the cache resource, no new FDs can be created for this resource until we are done
		if !used {
			os.Remove(item.Path)
			quota -= item.Size
		}

		// This allows a new open request to re-create the cache resource and serve a new file handle
		unlock()

		if quota < 0 {
			break
		}

	}

}

// Go routine to monitor cache at regular intervals and preform cleanup as needed
func (mfs *MinFS) MonitorCache() {
	fmt.Println("Starting cache monitor: quota =", mfs.config.quota, "GB")
	defer mfs.m.Unlock()

	MAX_SIZE := float64(mfs.config.quota)

	for {
		select {

		case <-time.After(30 * time.Second):
			items, size, err := DirSize(mfs.config.cache)
			if err != nil {
				mfs.log.Println("Error in lstating cache directory...it's likely in flux:", err)
			} else if size <= MAX_SIZE {
				mfs.log.Println("Cache OK: Cache files:", len(items), "Size:", size, "GB Open Files:", len(mfs.openfds))
			} else {
				mfs.log.Println("Cache OVERLOAD: Cache files:", len(items), "Size:", size, "GB Open Files:", len(mfs.openfds))
				mfs.DeleteUntilQuota(items, size-MAX_SIZE)
			}

		}
	}
}
