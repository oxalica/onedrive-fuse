{
  description = "Mount Microsoft OneDrive storage as FUSE filesystem";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, flake-utils, nixpkgs }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        inherit (nixpkgs) lib;
        pkgs = nixpkgs.legacyPackages.${system};
        manifest = lib.importTOML (self + "/Cargo.toml");

        inherit (manifest.package) name version;

      in rec {
        packages = rec {
          ${name} = default;
          default = pkgs.rustPlatform.buildRustPackage {
            pname = name;
            inherit version;

            nativeBuildInputs = with pkgs; [ pkg-config ];
            buildInputs = with pkgs; [ fuse openssl ];

            src = self;
            cargoLock.lockFile = self + "/Cargo.lock";

            meta.license = lib.licenses.mit;
          };
        };

        devShells.default = pkgs.mkShell {
          inputsFrom = [ packages.default ];
          RUST_BACKTRACE = 1;
        };
      });
}
