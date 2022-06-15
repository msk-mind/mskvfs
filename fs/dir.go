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

func (dir *Dir) needsScan() bool {
	return !dir.scanned
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

func (dir *Dir) storeFile(bucket *meta.Bucket, tx *meta.Tx, baseKey string, objInfo minio.ObjectInfo) error {
	var f File
	err := bucket.Get(baseKey, &f)
	if err == nil {
		// Object already exists and accessible, update values as needed.
		f.dir = dir
		f.mfs = dir.mfs
		f.Size = uint64(objInfo.Size)
		f.ETag = objInfo.ETag
		if objInfo.LastModified.After(f.Chgtime) {
			f.Chgtime = objInfo.LastModified
		}
		if objInfo.LastModified.After(f.Crtime) {
			f.Crtime = objInfo.LastModified
		}
		if objInfo.LastModified.After(f.Mtime) {
			f.Mtime = objInfo.LastModified
		}
		if objInfo.LastModified.After(f.Atime) {
			f.Atime = objInfo.LastModified
		}
	} else if meta.IsNoSuchObject(err) {
		// Object not found, allocate a new inode.
		var seq uint64
		seq, err = dir.mfs.NextSequence(tx)
		if err != nil {
			return err
		}
		f = File{
			dir:     dir,
			Path:    baseKey,
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
		if err = f.store(tx); err != nil {
			return err
		}
	} // else {
	// Returns failure for all other errors.
	return err
}

func (dir *Dir) storeBucket(bucket *meta.Bucket, tx *meta.Tx, baseKey string, objInfo minio.BucketInfo) error {
	var d Dir
	err := bucket.Get(baseKey, &d)
	if err == nil {
		// Prefix already exists and accessible, update values as needed.
		d.dir = dir
		d.mfs = dir.mfs
	} else if meta.IsNoSuchObject(err) {
		// Prefix not found allocate a new inode and create a new directory.
		var seq uint64
		seq, err = dir.mfs.NextSequence(tx)
		if err != nil {
			return err
		}
		d = Dir{
			dir:   dir,
			Path:  baseKey,
			Inode: seq,
			Mode:  0770 | os.ModeDir,
			GID:   dir.mfs.config.gid,
			UID:   dir.mfs.config.uid,
		}
		if err = d.store(tx); err != nil {
			return err
		}
	} // else {
	// For all other errors this operation fails.
	return err
}

func (dir *Dir) storeDir(bucket *meta.Bucket, tx *meta.Tx, baseKey string, objInfo minio.ObjectInfo) error {
	var d Dir
	err := bucket.Get(baseKey, &d)
	if err == nil {
		// Prefix already exists and accessible, update values as needed.
		d.dir = dir
		d.mfs = dir.mfs
	} else if meta.IsNoSuchObject(err) {
		// Prefix not found allocate a new inode and create a new directory.
		var seq uint64
		seq, err = dir.mfs.NextSequence(tx)
		if err != nil {
			return err
		}
		d = Dir{
			dir:   dir,
			Path:  baseKey,
			Inode: seq,
			Mode:  0770 | os.ModeDir,
			GID:   dir.mfs.config.gid,
			UID:   dir.mfs.config.uid,

			Chgtime: objInfo.LastModified,
			Crtime:  objInfo.LastModified,
			Mtime:   objInfo.LastModified,
			Atime:   objInfo.LastModified,
		}
		if err = d.store(tx); err != nil {
			return err
		}
	} // else {
	// For all other errors this operation fails.
	return err
}

func (dir *Dir) scanRoot(ctx context.Context, Uid uint32) (entries []Dir, err error) {
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

func (dir *Dir) scanBucket(ctx context.Context, uid uint32) (entries []Dir, err error) {

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
				Mode:  0770 | os.ModeDir,
				GID:   dir.mfs.config.gid,
				UID:   dir.mfs.config.uid,
			}

			entries = append(entries, d)
		}
		// 	dir.storeDir(b, tx, baseKey, objInfo)
		// } else {
		// 	dir.storeFile(b, tx, baseKey, objInfo)
		// }
	}

	return entries, nil
}

// ReadDirAll will return all files in current dir
func (dir *Dir) ReadDirAll(ctx context.Context, uid uint32) (entries []fuse.Dirent, err error) {
	fmt.Println("ReadDirAll(), dir.RemotePath =", dir.RemotePath(), ",uid =", uid)

	var scanDirs = []Dir{}

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

	var scanDirs = []Dir{}

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
		if scanDirs[idx].Path == name {
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

func (dir *Dir) bucket(tx *meta.Tx) *meta.Bucket {
	// Root folder.
	if dir.dir == nil {
		return tx.Bucket("minio/")
	}

	b := dir.dir.bucket(tx)

	return b.Bucket(dir.Path + "/")
}

// Mkdir will make a new directory below current dir
func (dir *Dir) Mkdir(ctx context.Context, req *fuse.MkdirRequest) (fs.Node, error) {
	subdir := Dir{
		dir: dir,
		mfs: dir.mfs,

		Path: req.Name,

		Mode: 0770 | os.ModeDir,
		GID:  dir.mfs.config.gid,
		UID:  dir.mfs.config.uid,

		Chgtime: time.Now(),
		Crtime:  time.Now(),
		Mtime:   time.Now(),
		Atime:   time.Now(),
	}

	tx, err := dir.mfs.db.Begin(true)
	if err != nil {
		return nil, err
	}

	defer tx.Rollback()

	if err := subdir.store(tx); err != nil {
		return nil, err
	}

	// Commit the transaction and check for error.
	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return &subdir, nil
}

// Remove will delete a file or directory from current directory
func (dir *Dir) Remove(ctx context.Context, req *fuse.RemoveRequest) error {
	if err := dir.mfs.wait(path.Join(dir.FullPath(), req.Name)); err != nil {
		return err
	}

	tx, err := dir.mfs.db.Begin(true)
	if err != nil {
		return err
	}

	defer tx.Rollback()

	b := dir.bucket(tx)

	var o interface{}
	if err := b.Get(req.Name, &o); meta.IsNoSuchObject(err) {
		return fuse.ENOENT
	} else if err != nil {
		return err
	} else if err := b.Delete(req.Name); err != nil {
		return err
	}

	if req.Dir {
		b.DeleteBucket(req.Name + "/")
	}

	// if err := dir.mfs.api.RemoveObject(ctx, dir.mfs.config.bucket, path.Join(dir.RemotePath(), req.Name), minio.RemoveObjectOptions{}); err != nil {
	// 	return err
	// }

	return tx.Commit()
}

// store the dir object in cache
func (dir *Dir) store(tx *meta.Tx) error {
	// directories will be stored in their parent buckets
	b := dir.dir.bucket(tx)

	subbucketPath := path.Base(dir.Path)
	if _, err := b.CreateBucketIfNotExists(subbucketPath + "/"); err != nil {
		return err
	}
	fmt.Printf("Storing %v at %s as %T\n", dir, subbucketPath, dir)

	return b.Put(subbucketPath, dir)
}

// Dirent will return the fuse Dirent for current dir
func (dir *Dir) Dirent() fuse.Dirent {
	return fuse.Dirent{
		Inode: dir.Inode, Name: dir.Path, Type: fuse.DT_Dir,
	}
}

// Create will return a new empty file in current dir, if the file is currently locked, it will
// wait for the lock to be freed.
func (dir *Dir) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
	if err := dir.mfs.wait(path.Join(dir.FullPath(), req.Name)); err != nil {
		return nil, nil, err
	}

	tx, err := dir.mfs.db.Begin(true)
	if err != nil {
		return nil, nil, err
	}

	defer tx.Rollback()

	b := dir.bucket(tx)

	name := req.Name

	var f File
	if gerr := b.Get(name, &f); gerr == nil {
		f.mfs = dir.mfs
		f.dir = dir
	} else if i, nerr := dir.mfs.NextSequence(tx); nerr != nil {
		return nil, nil, nerr
	} else {
		f = File{
			mfs: dir.mfs,
			dir: dir,

			Size:    uint64(0),
			Inode:   i,
			Path:    req.Name,
			Mode:    req.Mode, // dir.mfs.config.mode, // should we use same mode for scan?
			UID:     dir.mfs.config.uid,
			GID:     dir.mfs.config.gid,
			Chgtime: time.Now().UTC(),
			Crtime:  time.Now().UTC(),
			Mtime:   time.Now().UTC(),
			Atime:   time.Now().UTC(),
			ETag:    "",

			// req.Umask
		}
	}

	if serr := f.store(tx); serr != nil {
		return nil, nil, serr
	}

	var fh *FileHandle
	if fh, err = dir.mfs.Acquire(&f, f.FullPath()); err != nil {
		return nil, nil, err
	}
	fh.dirty = true
	if fh.cachePath, err = dir.mfs.NewCachePath(); err != nil {
		return nil, nil, err
	}
	if fh.File, err = os.OpenFile(fh.cachePath, int(req.Flags), dir.mfs.config.mode); err != nil {
		return nil, nil, err
	}

	// Commit the transaction and check for error.
	if err = tx.Commit(); err != nil {
		return nil, nil, err
	}

	resp.Handle = fuse.HandleID(fh.handle)
	return &f, fh, nil
}

// Rename will rename files
func (dir *Dir) Rename(ctx context.Context, req *fuse.RenameRequest, nd fs.Node) error {
	fmt.Println("Rename() not allowed")
	return nil
}
