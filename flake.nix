{
  description = "Mount your Microsoft OneDrive storage as FUSE filesystem";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, flake-utils, nixpkgs }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        inherit (builtins) readFile fromTOML;
        pkgs = nixpkgs.legacyPackages.${system};
        manifest = fromTOML (readFile ./Cargo.toml);

        inherit (manifest.package) name version;
        nativeBuildInputs = with pkgs; [ pkg-config ];
        buildInputs = with pkgs; [ fuse openssl ];

      in {
        packages = rec {
          ${name} = default;
          default = pkgs.rustPlatform.buildRustPackage {
            pname = name;
            inherit version nativeBuildInputs buildInputs;

            src = ./.;
            cargoLock.lockFile = ./Cargo.lock;

            meta.license = nixpkgs.lib.licenses.mit;
          };
        };

        devShells.default = pkgs.mkShell {
          inherit nativeBuildInputs buildInputs;
          RUST_BACKTRACE = 1;
        };
      });
}
