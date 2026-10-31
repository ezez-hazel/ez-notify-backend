{
  description = "EZ Notify - The next generation of bilibili notification";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-26.05";
    flake-utils.url = "github:numtide/flake-utils";
    
    # 用于获取精细化、可定制的 Rust 工具链
    fenix = {
      url = "github:nix-community/fenix";
      inputs.nixpkgs.follows = "nixpkgs";
    };

    # 用于高效构建 Rust 项目的工具
    crane = {
      url = "github:ipetkov/crane";
    };
  };

  outputs = { self, nixpkgs, flake-utils, fenix, crane, ... }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs {
          inherit system;
        };

        # 选择 Rust 工具链（这里选择稳定版 stable，你可以根据需要换成 minimal、complete 或特定版本）
        rustToolchain = fenix.packages.${system}.complete.withComponents [
          "cargo"
          "rustc"
          "rust-std"
          "rustfmt"
          "clippy"
          "rust-analyzer"
        ];

        # 将 crane 与我们选择的工具链结合
        craneLib = (crane.mkLib pkgs).overrideToolchain rustToolchain;

        # 过滤源码，只保留构建 Rust 项目必要的文件（排除 target、git 等无关文件，提高缓存命中率）
        src = craneLib.cleanCargoSource ./.;

        # 公共参数
        commonArgs = {
          inherit src;
          strictDeps = true;

          # 如果项目有系统依赖（比如 openssl、pkg-config 等），可以在这里添加：
          # buildInputs = with pkgs; [ openssl ];
          # nativeBuildInputs = with pkgs; [ pkg-config ];
        };

        # 1. 仅构建依赖（用于加速后续的增量构建缓存）
        cargoArtifacts = craneLib.buildDepsOnly commonArgs;

        # 2. 构建真正的 Rust 包
        ezNotify = craneLib.buildPackage (commonArgs // {
          inherit cargoArtifacts;
        });
      in
      {
        # 定义 nix包（执行 `nix build` 时构建的内容）
        packages.${system}.default = ezNotify;

        # 导出 NixOS 模块，并将 ezNotify 作为参数透传给 Service
        nixosModules.default = { pkgs, ... }: {
          imports = [
            ./modules/ez-notify.nix
          ];

          _module.args.ezNotifyPackage =
            self.packages.${pkgs.system}.default;
        };

        # 定义开发环境（执行 `nix develop` 或通过 direnv 进入的环境）
        devShells.default = pkgs.mkShell {
          inputsFrom = [ ezNotify ];

          packages = [
            rustToolchain
            pkgs.cargo-edit   # 方便用命令行管理依赖（cargo add/rm）
            pkgs.cargo-outdated
          ];

          # 设置一些环境变量（可选）
          RUST_BACKTRACE = "1";
        };
      }
    );
}