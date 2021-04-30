# onedrive-fuse

[![crates.io](https://img.shields.io/crates/v/onedrive-fuse.svg)](https://crates.io/crates/onedrive-fuse)

Mount your [Microsoft OneDrive][onedrive] storage as [FUSE] filesystem.

[onedrive]: https://products.office.com/en-us/onedrive/online-cloud-storage
[FUSE]: https://github.com/libfuse/libfuse

## Usage

Use your package manager to install dependencies:
- openssl
- fuse (libfuse)

Install it from crates.io:
```bash
cargo install onedrive-fuse
```

For the first time, you should register your own Application (Client) ID for the API access.
Read [`doc/register_app.md`](./doc/register_app.md) for steps.

Login to your OneDrive account with Client ID of your own application.
This will prompt a browser window to ask you to login your Microsoft account for OneDrive.
After a successful login, the page will redirect your to a blank page whose URL contains `nativeclient?code=`,
then copy the **FULL** URL, paste it to the terminal and press enter to login.
```bash
# Login with read-only access.
onedrive-fuse login --client-id <paste-your-client-id-here>
# Or login with read-write access.
# onedrive-fuse login --read-write --client-id <paste-your-client-id-here>
```

Your access token will be saved to to XDG config directory,
which is default to be `~/.config/onedrive-fuse/credential.json`.

Finally, mount your OneDrive to a empty directory.
It works in foreground by default, the terminal window should be kept opened.
It will fetch the whole tree hierarchy before mounting,
please wait for `FUSE initialized` to be shown and the filesystem is now mounted.
```bash
mkdir -p ~/onedrive # The directory to be mounted should be empty.
onedrive-fuse mount ~/onedrive
# Or if you want to read-writea access. The token must be authorized to have read-write access.
# Use it with care. Bugs may corrupt your OneDrive data.
# onedrive-fuse mount ~/onedrive -o permission.readonly=false
```

Umount the fuse filesystem gracefully.
You should **NOT** directly `Ctrl-C` or kill the `onedrive-fuse` instance.

> Warning: Wait your upload session to be finished before umounting the filesystem.
> Currently we don't yet implemented waiting for uploading before shutdown.

```bash
fusermount -u ~/onedrive
```

## Features implemented

- [x] FUSE syscalls
  - [x] Read
    - [x] access
    - [x] forget
    - [x] getattr
    - [x] lookup
    - [x] open
      - [x] O_RDONLY
    - [x] opendir
    - [x] read
    - [x] readdir
    - [x] release
    - [x] releasedir
    - [x] statfs
  - [x] Write
    - [x] create
    - [x] mkdir
    - [x] open
      - [x] O_WRONLY/O_RDWR
      - [x] O_TRUNC
      - [x] O_EXCL
    - [x] rename
    - [x] rmdir
    - [x] setattr
      - [x] size
      - [x] mtime
    - [x] unlink
    - [x] write
  - [x] Other
    - destroy
    - flush
    - [x] fsync
    - [x] fsyncdir
    - init
  - Unsupported
    - bmap
    - getlk
    - getxattr
    - link
    - listxattr
    - mknod
    - readlink
    - removexattr
    - setlk
    - setxattr
    - symlink
- [x] Cache
  - [x] Statfs cache
  - [x] Inode attributes (stat) cache
  - [x] Directory tree cache
  - [x] Sync remote changes with local cache
  - [x] File read cache
  - [x] File write cache/buffer

## License

GPL-3.0
