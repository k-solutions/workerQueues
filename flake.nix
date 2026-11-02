{
  description = "workerQueue: Sidekiq -style background workers layer (GHC 9.12)";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs { inherit system; };
        hlib = pkgs.haskell.lib.compose;
        relax = pkg: hlib.dontCheck (hlib.doJailbreak (hlib.markUnbroken pkg));

        # Pin the GHC 9.12 package set (GHC 9.12 / base-4.21)
        hpkgs = pkgs.haskell.packages.ghc912.override {
          overrides = hfinal: hprev: {
            # Transitive dependencies with overly strict base < 4.21 bounds on GHC 9.12:
            pqueue        = relax hprev.pqueue;
            beam-core     = relax hprev.beam-core;
            beam-migrate  = relax hprev.beam-migrate;
            beam-postgres = relax hprev.beam-postgres;
            beam-sqlite   = relax hprev.beam-sqlite;

            # Register our local package via callCabal2nix
            workerQueues = hfinal.callCabal2nix "workerQueues" ./. { };
          };
        };
      in
      {
        # `nix build` builds the library, examples, and runs the test suite
        packages.default = hpkgs.workerQueues;

        # `nix develop` drops into a shell with GHC 9.12, HLS, Cabal, and C headers
        devShells.default = hpkgs.shellFor {
          packages = p: [ p.workerQueues ];

          # Bring in HLS compiled specifically against GHC 9.12 plus prebuilt CLI tools
          nativeBuildInputs = [
            hpkgs.ghc
            pkgs.cabal-install
            hpkgs.haskell-language-server
            hpkgs.fourmolu
            hpkgs.hlint
            pkgs.pkg-config
          ];

          # Native C libraries required by beam-postgres, beam-sqlite, and test suites
          buildInputs = [
            pkgs.postgresql.lib
            pkgs.postgresql
            pkgs.sqlite
            pkgs.zlib
          ];

          # Ensure Cabal and HLS can locate libpq and sqlite3 headers on NixOS
          shellHook = ''
            export PKG_CONFIG_PATH="${pkgs.postgresql.lib}/lib/pkgconfig:${pkgs.sqlite.dev}/lib/pkgconfig:$PKG_CONFIG_PATH"
            echo "Loaded GHC $(ghc --numeric-version) + HLS $(haskell-language-server-wrapper --numeric-version)"
          '';
        };
      }
    );
}
