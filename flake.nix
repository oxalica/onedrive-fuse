rec {
  description = "Mount Microsoft OneDrive storage as FUSE filesystem";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, flake-utils, nixpkgs }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        inherit (nixpkgs) lib;
        pkgs = nixpkgs.legacyPackages.${system};
        manifest = lib.importTOML (self + "/Cargo.toml");

      in {
        packages = rec {
          ${default.name} = default;
          default = pkgs.rustPlatform.buildRustPackage {
            pname = manifest.package.name;
            version = manifest.package.version;

            nativeBuildInputs = with pkgs; [ pkg-config ];
            buildInputs = with pkgs; [ fuse3 openssl ];

            src = self;
            cargoLock.lockFile = self + "/Cargo.lock";

            meta = {
              inherit description;
              homepage = manifest.package.repository;
              license = lib.licenses.gpl3Only;
            };
          };
        };

        devShells = rec {
          default = pkgs.mkShell {
            inputsFrom = [ self.packages.${system}.default ];

            RUST_BACKTRACE = 1;
          };

          without-rust = default.overrideAttrs (old: {
            nativeBuildInputs =
              lib.filter
                (drv: builtins.match ".*(rustc|cargo).*" drv.name == null)
                old.nativeBuildInputs;
          });
        };
      });
}
