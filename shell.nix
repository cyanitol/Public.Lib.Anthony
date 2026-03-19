{ pkgs ? import <nixpkgs> {} }:

pkgs.mkShell {
  buildInputs = with pkgs; [
    # Go toolchain (using latest available, go.mod requires 1.26.1)
    go_1_26
    gopls
    gotools
    go-tools
    delve
    codex

    # Build tools
    gnumake
    gcc

    # Git + GitHub CLI
    git
    gh

    # Fossil for SQLite source code
    fossil

    # SQLite for reference/testing
    sqlite
  ];

  shellHook = ''
    export GOPATH="$HOME/go"
    export PATH="$GOPATH/bin:$PATH"
    echo "Go SQLite Development Environment"
    echo "Go version: $(go version)"
  '';
}
