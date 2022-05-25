export CGO_ENABLED=0
export GOOS=linux
export GOARCH=amd64
BIN=mysql-connector
go build -o $BIN -ldflags $ROOT_DIR/main/proc_per_task/main.go