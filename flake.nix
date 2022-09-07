{
  description = "Mount Microsoft OneDrive storage as FUSE filesystem";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, flake-utils, nixpkgs }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        inherit (builtins) readFile fromTOML;
        pkgs = nixpkgs.legacyPackages.${system};
        manifest = fromTOML (readFile (self + "/Cargo.toml"));

        inherit (manifest.package) name version;

      in {
        packages = rec {
          ${name} = default;
          default = pkgs.rustPlatform.buildRustPackage {
            pname = name;
            inherit version;

            nativeBuildInputs = with pkgs; [ pkg-config ];
            buildInputs = with pkgs; [ fuse' openssl ];

            src = self;
            cargoLock.lockFile = self + "/Cargo.lock";

            meta.license = nixpkgs.lib.licenses.mit;
          };
        };

        devShells.default = pkgs.mkShell {
          nativeBuildInputs = with pkgs; [ pkg-config ];
          buildInputs = with pkgs; [
            openssl
            # Remove `/bin` to avoid conflict with SUID `fusermount` from system PATH.
            (symlinkJoin {
              name = "fuse-without-bin";
              paths = [ fuse ];
              postBuild = ''
                rm -r $out/bin
              '';
            })
          ];

          RUST_BACKTRACE = 1;
        };
      });
}
