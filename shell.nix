with import <nixpkgs> {};
mkShell {
  buildInputs = [ pkg-config fuse openssl ];
}
