# Natshed

## Usage

```bash
export NATS_URL=localhost:30042
```

```bash
go run cmd/main.go worker 
```

```bash
go run cmd/main.go client --task-id task1 --duration 1m --max-occurrences=3 
```

