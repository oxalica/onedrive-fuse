rec {
  description = "Mount Microsoft OneDrive storage as FUSE filesystem";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    rust-overlay.url = "github:oxalica/rust-overlay";
    rust-overlay.inputs.flake-utils.follows = "flake-utils";
    rust-overlay.inputs.nixpkgs.follows = "nixpkgs";
  };

  outputs = { self, flake-utils, nixpkgs, rust-overlay }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        inherit (nixpkgs) lib;
        pkgs = nixpkgs.legacyPackages.${system};
        manifest = lib.importTOML (self + "/Cargo.toml");

        inherit (manifest.package) name version;

        nativeBuildInputs = with pkgs; [ pkg-config ];
        buildInputs = with pkgs; [ fuse3 openssl sqlite ];

      in {
        packages = rec {
          ${name} = default;
          default = pkgs.rustPlatform.buildRustPackage {
            pname = name;
            inherit version nativeBuildInputs buildInputs;

            src = self;
            cargoLock.lockFile = self + "/Cargo.lock";

            meta = {
              inherit description;
              homepage = manifest.package.repository;
              license = lib.licenses.gpl3Only;
            };
          };
        };

        devShells.default = pkgs.mkShell {
          nativeBuildInputs = nativeBuildInputs ++ [
            rust-overlay.packages.${system}.rust
            pkgs.sqlite-interactive
          ];
          inherit buildInputs;

          RUST_BACKTRACE = 1;
        };
      });
}
