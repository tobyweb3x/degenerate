PROTO_FILES := $(shell find . -name "*.proto")
GO_OUT_DIR=../web/protos


golang:
	@mkdir -p ../web/protos/
	@protoc \
		--proto_path=. \
		--go_out=$(GO_OUT_DIR) --go_opt=paths=source_relative \
		--go-grpc_out=$(GO_OUT_DIR) --go-grpc_opt=paths=source_relative \
		$(PROTO_FILES)
	@echo "✅ Go protos generated in $(GO_OUT_DIR)"

	 