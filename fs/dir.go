// Copyright (c) 2021 MinIO, Inc.
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
	"context"
	"fmt"
	"os"
	"path"
	"strings"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"

	"github.com/minio/minfs/meta"
	minio "github.com/minio/minio-go/v7"
)

type FilesystemElement interface {
	Dirpath() string
	Dirent() fuse.Dirent
}

// Dir implements both Node and Handle for the root directory.
type Dir struct {
	mfs *MinFS

	dir *Dir

	Path  string
	Inode uint64
	Mode  os.FileMode

	Size uint64
	ETag string

	Atime time.Time
	Mtime time.Time

	UID uint32
	GID uint32

	// OS X only
	Bkuptime time.Time
	Chgtime  time.Time
	Crtime   time.Time
	Flags    uint32 // see chflags(2)

	scanned bool
}

// Attr returns the attributes for the directory
func (dir *Dir) Attr(ctx context.Context, a *fuse.Attr) error {
	*a = fuse.Attr{
		Inode:  dir.Inode,
		Size:   dir.Size,
		Atime:  dir.Atime,
		Mtime:  dir.Mtime,
		Ctime:  dir.Chgtime,
		Crtime: dir.Crtime,
		Mode:   dir.Mode,
		Uid:    dir.UID,
		Gid:    dir.GID,
		Flags:  dir.Flags,
	}

	return nil
}

// RemotePath returns the full path including parent paths for current dir on the remote
func (dir *Dir) RemotePath() string {
	return path.Join(dir.mfs.config.basePath, dir.FullPath())
}

// FullPath returns the full path including parent paths for current dir
func (dir *Dir) FullPath() string {
	fullPath := ""

	p := dir
	for {
		if p == nil {
			break
		}

		fullPath = path.Join(p.Path, fullPath)

		p = p.dir
	}

	return fullPath
}

// Dirent will return the fuse Dirent for current dir
func (dir Dir) Dirent() fuse.Dirent {
	return fuse.Dirent{
		Inode: dir.Inode, Name: dir.Path, Type: fuse.DT_Dir,
	}
}

// Dirent will return the fuse Dirent for current dir
func (dir Dir) Dirpath() string {
	return dir.Path
}

func (dir *Dir) scanRoot(ctx context.Context, Uid uint32) (entries []FilesystemElement, err error) {
	fmt.Println(" +- scanRoot()")

	prefix := dir.RemotePath()
	if prefix != "" {
		prefix = prefix + "/"
	}

	api, err := dir.mfs.getApi(Uid)
	if err != nil {
		return nil, err
	}

	ch, err := api.ListBuckets(ctx)
	if err != nil {
		return nil, err
	}

	var seq uint64

	for idx := range ch {

		key := ch[idx].Name
		seq += 1

		var d = Dir{
			dir:   dir,
			Path:  key,
			Inode: seq,
			Mode:  0770 | os.ModeDir,
			GID:   dir.mfs.config.gid,
			UID:   dir.mfs.config.uid,
		}

		entries = append(entries, d)
	}

	return entries, nil
}

func (dir *Dir) scanBucket(ctx context.Context, uid uint32) (entries []FilesystemElement, err error) {

	bucket := strings.Split(dir.RemotePath(), "/")[0] // Bucket will always be given as first part of remote path

	prefix := strings.Replace(dir.RemotePath()+"/", bucket+"/", "", 1) // We need prefix paths to end in / if they aren't empty

	fmt.Println(" +- scanBucket():", dir.RemotePath(), ", bucket=", bucket, ",prefix=", prefix)

	api, err := dir.mfs.getApi(uid)
	if err != nil {
		return nil, err
	}

	ch := api.ListObjects(ctx, bucket, minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: false,
	})

	var seq uint64

	for objInfo := range ch {
		key := objInfo.Key[len(prefix):]

		seq += 1

		path := path.Base(key)

		fmt.Println(" +- Found Object:", "Key:", objInfo.Key, "Used key:", key, "path:", path)

		// object still exists

		if strings.HasSuffix(key, "/") {
			var d = Dir{
				dir:   dir,
				Path:  path,
				Inode: seq,
				Mode:  0555 | os.ModeDir,
				GID:   dir.mfs.config.gid,
				UID:   dir.mfs.config.uid,
			}

			entries = append(entries, d)
		} else {
			var f = File{
				dir:     dir,
				Path:    path,
				Size:    uint64(objInfo.Size),
				Inode:   seq,
				Mode:    dir.mfs.config.mode,
				GID:     dir.mfs.config.gid,
				UID:     dir.mfs.config.uid,
				Chgtime: objInfo.LastModified,
				Crtime:  objInfo.LastModified,
				Mtime:   objInfo.LastModified,
				Atime:   objInfo.LastModified,
				ETag:    objInfo.ETag,
			}
			entries = append(entries, f)
		}
	}

	return entries, nil
}

// ReadDirAll will return all files in current dir
func (dir *Dir) ReadDirAll(ctx context.Context, uid uint32) (entries []fuse.Dirent, err error) {
	fmt.Println("ReadDirAll(), dir.RemotePath =", dir.RemotePath(), ",uid =", uid)

	var scanDirs []FilesystemElement

	switch dir.Path {
	case "":
		scanDirs, err = dir.scanRoot(ctx, uid)
		if err != nil {
			return nil, err
		}
	default:
		scanDirs, err = dir.scanBucket(ctx, uid)
		if err != nil {
			return nil, err
		}
	}

	for _, x := range scanDirs {
		entries = append(entries, x.Dirent())
	}

	fmt.Println("Completed ReadDirAll:", entries)

	return entries, nil

}

// Lookup returns the file node, and scans the current dir if necessary
func (dir *Dir) Lookup(ctx context.Context, name string, uid uint32) (node fs.Node, err error) {
	fmt.Println("Lookup():, dir.Path =", dir.Path, ", name =", name, ", uid = ", uid)

	var scanDirs []FilesystemElement

	switch dir.Path {
	case "":
		scanDirs, err = dir.scanRoot(ctx, uid)
		if err != nil {
			return nil, err
		}
	default:
		scanDirs, err = dir.scanBucket(ctx, uid)
		if err != nil {
			return nil, err
		}
	}

	fmt.Println("Completed scan:", scanDirs)

	// I Have zero clue what this interface business is,
	var o interface{} // Okay i like it, Picasso
	for idx := range scanDirs {
		if scanDirs[idx].Dirpath() == name {
			o = (scanDirs[idx])
		}
	}

	if file, ok := o.(File); ok {
		fmt.Println("file=", file)
		file.mfs = dir.mfs
		file.dir = dir
		return &file, nil
	} else if subdir, ok := o.(Dir); ok {
		fmt.Println("subdir=", subdir)
		subdir.mfs = dir.mfs
		subdir.dir = dir
		return &subdir, nil
	}

	return nil, fuse.ENOENT
}

// Mkdir will make a new directory below current dir
func (dir *Dir) Mkdir(ctx context.Context, req *fuse.MkdirRequest) (fs.Node, error) {
	fmt.Println("Mkdir() not allowed")
	return nil, nil
}

// Remove will delete a file or directory from current directory
func (dir *Dir) Remove(ctx context.Context, req *fuse.RemoveRequest) error {
	fmt.Println("Remove() not allowed")
	return nil
}

// Create will return a new empty file in current dir, if the file is currently locked, it will
// wait for the lock to be freed.
func (dir *Dir) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
	fmt.Println("Create() not allowed")
	return nil, nil, nil
}

// Rename will rename files
func (dir *Dir) Rename(ctx context.Context, req *fuse.RenameRequest, nd fs.Node) error {
	fmt.Println("Rename() not allowed")
	return nil
}

func (dir *Dir) bucket(tx *meta.Tx) *meta.Bucket {
	// Root folder.
	if dir.dir == nil {
		return tx.Bucket("minio/")
	}

	b := dir.dir.bucket(tx)

	return b.Bucket(dir.Path + "/")
}
