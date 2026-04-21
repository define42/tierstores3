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

Start the gateway:

```bash
./tierstore gateway -listen :9000 -state ./tierstore-state.json -replicas 2 -cold-after 720h
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

The rebalance worker will move replicas to the new desired placement over time.

## Design notes

- Objects are first written to the **hot** tier.
- `last_accessed_at` is updated asynchronously by the gateway.
- The tiering worker demotes hot objects to the **warm** tier after `cold-after` worth of inactivity.
- If `-promote-on-read=true`, reads of warm objects trigger best-effort promotion back to hot storage.
- Placement is computed from `(bucket, key)` plus the current node map; there is no central per-object placement table.
- Rebalancing is done in the background by comparing the current replica set against the desired replica set.

## Important gaps

This is intentionally a prototype. It does **not** yet include:

- S3 auth / SigV4
- multipart upload
- versioning
- erasure coding
- TLS and mTLS between gateway and nodes
- per-object retention / legal hold
- distributed consensus for the metadata store
- node-to-node copy shortcuts
- efficient streaming fanout on upload
- access-log compaction beyond a simple in-memory coalescer

For production, I would replace the single JSON metadata file with a real replicated metadata service and split the gateway/control plane from the storage plane more aggressively.
