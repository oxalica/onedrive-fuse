# onedrive-fuse

:warning: This branch is experimental for the rewrite of the whole VFS module.
Check `master` branch for the latest stable version.

[![crates.io](https://img.shields.io/crates/v/onedrive-fuse.svg)](https://crates.io/crates/onedrive-fuse)

Mount [Microsoft OneDrive][onedrive] storage as [FUSE] filesystem.

[onedrive]: https://products.office.com/en-us/onedrive/online-cloud-storage
[FUSE]: https://github.com/libfuse/libfuse

## Installation

*Note: For Nix users, the program is already packaged via Nix Flake in `flake.nix`.*

1.  Use your package manager to install these dependencies:
    - pkg-config
    - openssl
    - fuse (libfuse)

1.  Compile and install the program from crates.io:

    ```
    $ cargo install onedrive-fuse
    ```

## Prepare

1.  For the first time, you should register your own Application (Client) ID for the API access.
    Read [`doc/register_app.md`](./doc/register_app.md) for detail steps.

1.  Login to your OneDrive account with the Client ID of your own application from the previous step.
    By default, we only request for read-only access.

    ```
    $ onedrive-fuse login --client-id <paste-your-client-id-here>
    ```

    If you want read-write access, you should instead run,
    ```
    $ onedrive-fuse login --read-write --client-id <paste-your-client-id-here>
    ```

    This will prompt a browser window to ask you to login your Microsoft account for OneDrive.
    After a successful login, the page will redirect your to a blank page
    whose URL contains `nativeclient?code=`.
    Copy the **FULL** URL, and paste it to the terminal and press enter to login.

    Your access token will be saved under [XDG config directory][xdg-dirs],
    which is by default `~/.config/onedrive-fuse/credential.json`.
    So you don't need to re-login every time.
    But if you are away for too long, eg. for months, you might have to re-login.

    [xdg-dirs]: https://specifications.freedesktop.org/basedir-spec/basedir-spec-latest.html

## Usage

### Manual

1.  Create an empty directory as mount point, like `~/onedrive`,
    and mount your OneDrive storage on it.
    By default the mounted storage is readonly.

    ```
    $ mkdir -p ~/onedrive # The directory to be mounted should be empty.
    $ onedrive-fuse mount ~/onedrive
    ```

    If you want to mount with read-write access,
    you must also request for read-write access in the previous login step.
    Then mount the storage with,

    ```
    onedrive-fuse mount ~/onedrive -o permission.readonly=false
    ```

    **:warning: Use read-write permission with care! Bugs may corrupt your files in OneDrive!**

1.  Once it's started, wait for seconds for initialization until `FUSE initialized` displayed,
    indicating the filesystem is ready now.
    You can do whatever you want under the mount point.

    The program runs in foreground by default, the terminal window should be kept open.
    You may need other mechanism like `systemd` (see below) to make it run background.

1.  If you are done for, run this command to umount the filesystem gracefully.
    You should **NOT** directly `Ctrl-C` or kill the `onedrive-fuse` instance,
    it usually cause data loss.

    ```
    $ fusermount -u ~/onedrive
    ```

    **:warning: We havn't yet implemented auto-waiting for uploading before shutdown.
    Please wait your upload session to be finished before umounting the filesystem.
    Or your pending upload session would be cancelled.**

### Systemd

This program is integrated with [systemd] and is expected to be started as a user service.
See [`onedrive-fuse.service.example`](./onedrive-fuse.service.example)
for an example setup.
Note that you still need to manually [login](#prepare) first.

[systemd]: https://systemd.io

## FUSE features implemented

<details>

- [ ] FUSE syscalls
  - [ ] Read
    - [ ] forget
    - [x] getattr
    - [x] lookup
    - [ ] open
    - [x] opendir
    - [ ] read
    - [x] readdir
    - [x] readdirplus
    - [ ] release
    - [x] releasedir
    - [ ] statfs
  - [ ] Write
    - [ ] create
    - [ ] mkdir
    - [ ] open
    - [ ] rename
    - [ ] rmdir
    - [ ] setattr
    - [ ] unlink
    - [ ] write
  - [ ] Other
    - destroy
    - flush
    - [ ] fsync
    - [ ] fsyncdir
    - init
  - Unsupported
    - access
      - Permission check is done by kernel via `default_permissions`.
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

</details>

## License

GPL-3.0-only
