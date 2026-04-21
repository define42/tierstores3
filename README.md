# tierstore

`tierstore` is a Go prototype for the object-store design we discussed:

- dynamic node add/remove
- deterministic placement with weighted rendezvous hashing
- replication-based durability
- hot NVMe tier on ingest
- automatic demotion to warm HDD after an idle period
- optional promotion back to hot tier on read
- S3-like HTTP surface for buckets and objects

This is an MVP, not a production-complete S3 implementation.

## What it implements

The gateway exposes:

- `PUT /bucket` to create a bucket
- `PUT /bucket/key` to upload an object
- `GET /bucket/key` to fetch an object
- `HEAD /bucket/key` to read object metadata
- `DELETE /bucket/key` to delete an object
- `GET /` to list buckets
- `GET /bucket?list-type=2&prefix=...` to list objects

Admin endpoints:

- `POST /_admin/nodes` to add or update a storage node
- `POST /_admin/nodes/{id}/drain` to stop placing new replicas there
- `POST /_admin/nodes/{id}/activate` to return a node to service
- `DELETE /_admin/nodes/{id}` to remove a node from placement
- `GET /_admin/cluster` to inspect state

Each storage node exposes an internal API the gateway uses for hot/warm blob placement.

## Run it locally

Build:

```bash
go build ./cmd/tierstore
```

Start three nodes:

```bash
./tierstore node -id node1 -listen :9101 -hot-dir ./node1-nvme -warm-dir ./node1-hdd
./tierstore node -id node2 -listen :9102 -hot-dir ./node2-nvme -warm-dir ./node2-hdd
./tierstore node -id node3 -listen :9103 -hot-dir ./node3-nvme -warm-dir ./node3-hdd
```

Start three gateway/controller nodes:

```bash
./tierstore gateway \
  -gateway-id gw1 \
  -listen :9000 \
  -advertise-url http://127.0.0.1:9000 \
  -raft-addr 127.0.0.1:10001 \
  -raft-dir ./gw1-raft \
  -bootstrap \
  -join 'gw2=http://127.0.0.1:9001@127.0.0.1:10002,gw3=http://127.0.0.1:9002@127.0.0.1:10003' \
  -replicas 2 \
  -cold-after 720h

./tierstore gateway \
  -gateway-id gw2 \
  -listen :9001 \
  -advertise-url http://127.0.0.1:9001 \
  -raft-addr 127.0.0.1:10002 \
  -raft-dir ./gw2-raft \
  -join 'gw1=http://127.0.0.1:9000@127.0.0.1:10001,gw3=http://127.0.0.1:9002@127.0.0.1:10003' \
  -replicas 2 \
  -cold-after 720h

./tierstore gateway \
  -gateway-id gw3 \
  -listen :9002 \
  -advertise-url http://127.0.0.1:9002 \
  -raft-addr 127.0.0.1:10003 \
  -raft-dir ./gw3-raft \
  -join 'gw1=http://127.0.0.1:9000@127.0.0.1:10001,gw2=http://127.0.0.1:9001@127.0.0.1:10002' \
  -replicas 2 \
  -cold-after 720h
```

Add the nodes:

```bash
curl -sS -X POST http://127.0.0.1:9000/_admin/nodes \
  -H 'content-type: application/json' \
  -d '{"id":"node1","url":"http://127.0.0.1:9101","weight_hot":1,"weight_warm":1}'

curl -sS -X POST http://127.0.0.1:9000/_admin/nodes \
  -H 'content-type: application/json' \
  -d '{"id":"node2","url":"http://127.0.0.1:9102","weight_hot":1,"weight_warm":1}'

curl -sS -X POST http://127.0.0.1:9000/_admin/nodes \
  -H 'content-type: application/json' \
  -d '{"id":"node3","url":"http://127.0.0.1:9103","weight_hot":1,"weight_warm":1}'
```

Create a bucket and upload an object:

```bash
curl -X PUT http://127.0.0.1:9000/photos
curl -T ./pic.jpg http://127.0.0.1:9000/photos/2026/trip/pic.jpg
```

Read it back:

```bash
curl http://127.0.0.1:9000/photos/2026/trip/pic.jpg -o out.jpg
```

Inspect the cluster state:

```bash
curl http://127.0.0.1:9000/_admin/cluster | jq
```

Drain a node:

```bash
curl -X POST http://127.0.0.1:9000/_admin/nodes/node2/drain
```

The rebalance worker runs only on the current Raft leader and will move replicas to the new desired placement over time.

## Run with Docker Compose

Build and start the full 3-node, 3-gateway cluster:

```bash
docker compose up --build -d
```

The compose stack also starts a one-shot `cluster-init` container that waits for the Raft cluster to elect a leader and then registers all three storage nodes automatically.

Use any gateway on the host:

```bash
curl -X PUT http://127.0.0.1:9000/photos
curl -T ./pic.jpg http://127.0.0.1:9001/photos/demo/pic.jpg
curl http://127.0.0.1:9002/photos/demo/pic.jpg -o out.jpg
curl http://127.0.0.1:9000/_admin/cluster | jq
```

Stop and remove the cluster state:

```bash
docker compose down -v
```

## Design notes

- Objects are first written to the **hot** tier.
- `last_accessed_at` is updated asynchronously by the gateway cluster.
- The tiering worker demotes hot objects to the **warm** tier after `cold-after` worth of inactivity.
- If `-promote-on-read=true`, reads of warm objects trigger best-effort promotion back to hot storage.
- Placement is computed from `(bucket, key)` plus the current node map; there is no central per-object placement table.
- Gateway metadata and node state are replicated with embedded Raft, so any gateway can accept reads and followers transparently forward writes to the current leader.
- Rebalancing is done in the background by comparing the current replica set against the desired replica set.

## Important gaps

This is intentionally a prototype. It does **not** yet include:

- S3 auth / SigV4
- multipart upload
- versioning
- erasure coding
- TLS and mTLS between gateway and nodes
- per-object retention / legal hold
- node-to-node copy shortcuts
- efficient streaming fanout on upload
- access-log compaction beyond a simple in-memory coalescer
