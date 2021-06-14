{
  description = "fabric";

  inputs = {
    # nixpkgs.url = github:NixOS/nixpkgs/nixos-unstable;
    flakeutils.url = "github:numtide/flake-utils";
    naersk.url = "github:nmattia/naersk";
  };

  outputs = { self, nixpkgs, flakeutils, naersk }: 
    flakeutils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages."${system}";
        # naersk-lib = naersk.lib."${system}";
      in rec {

        # Operator (server)
        # packages.factordb = naersk-lib.buildPackage {
        #   pname = "factordb";
        #   src = self;
        #   root = ./.;

        #   buildInputs = with pkgs; [
        #     pkgconfig
        #   ];
        #   propagatedBuildInputs = with pkgs; [
        #     openssl
        #   ];
        #   runtimeDependencies = with pkgs; [
        #     openssl
        #   ];
        # };

        # defaultPackage = packages.factordb;

        # apps.factordb = flakeutils.lib.mkApp {
        #   drv = packages.factordb;
        # };

        # apps.cli = flakeutils.lib.mkApp {
        #   drv = packages.kube-workspace-cli;
        # };

        # defaultApp = apps.factordb;

        devShell = pkgs.stdenv.mkDerivation {
            name = "factordb";
            src = self;
            buildInputs = with pkgs; [
              pkgconfig
              gcc
            ];
            propagatedBuildInputs = with pkgs; [
              openssl
              pkgconfig
            ];

            # Allow `cargo run` etc to find ssl lib.
            LD_LIBRARY_PATH = "${pkgs.openssl.out}/lib:${pkgs.stdenv.cc.cc.lib}/lib64";
            RUST_BACKTRACE = "1";
            # Use lld linker for speedup.
            RUSTFLAGS = "-C link-arg=-fuse-ld=lld";
            RUST_LOG= "factor_core=trace";
        };
      }
    );
}  
