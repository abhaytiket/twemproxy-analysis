ARG GO_VERSION=1.25.0
FROM golang:${GO_VERSION}

# Install golangci-lint (linter) and gopls (language server)
RUN apt-get update && apt-get install -y curl git \
    && curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/HEAD/install.sh | sh -s -- -b $(go env GOPATH)/bin v2.4.0 \
    && go install golang.org/x/tools/gopls@latest \
    && apt-get clean && rm -rf /var/lib/apt/lists/*
