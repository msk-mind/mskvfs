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
	"time"
)

func DirSize(path string) (int64, error) {
	var size int64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return err
	})
	size = size / int64(math.Pow(1024.0, 3.0))
	return size, err
}

func (mfs *MinFS) MonitorCache() {
	fmt.Println("Starting cache monitor!")
	defer mfs.m.Unlock()

	for {
		select {

		case <-time.After(1 * time.Second):
			size, err := DirSize(mfs.config.cache)
			if err != nil {
				fmt.Println("Error getting cache director...")
			} else {
				fmt.Println("Current cache size:", size, "GB")

				mfs.m.Lock()
				fmt.Println("Open FDs:", len(mfs.openfds))
				mfs.m.Unlock()
			}

		}
	}
}
