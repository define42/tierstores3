package gateway

import (
	"encoding/xml"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"tierstore/internal/model"
)

type listAllMyBucketsResult struct {
	XMLName xml.Name     `xml:"ListAllMyBucketsResult"`
	Xmlns   string       `xml:"xmlns,attr"`
	Buckets bucketsBlock `xml:"Buckets"`
}

type bucketsBlock struct {
	Buckets []bucketXML `xml:"Bucket"`
}

type bucketXML struct {
	Name         string `xml:"Name"`
	CreationDate string `xml:"CreationDate"`
}

type listBucketResultV2 struct {
	XMLName               xml.Name        `xml:"ListBucketResult"`
	Xmlns                 string          `xml:"xmlns,attr"`
	Name                  string          `xml:"Name"`
	Prefix                string          `xml:"Prefix"`
	KeyCount              int             `xml:"KeyCount"`
	MaxKeys               int             `xml:"MaxKeys"`
	Delimiter             string          `xml:"Delimiter,omitempty"`
	IsTruncated           bool            `xml:"IsTruncated"`
	ContinuationToken     string          `xml:"ContinuationToken,omitempty"`
	NextContinuationToken string          `xml:"NextContinuationToken,omitempty"`
	Contents              []objectContent `xml:"Contents,omitempty"`
	CommonPrefixes        []commonPrefix  `xml:"CommonPrefixes,omitempty"`
}

type objectContent struct {
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
	StorageClass string `xml:"StorageClass"`
}

type commonPrefix struct {
	Prefix string `xml:"Prefix"`
}

func (s *Server) handleListBuckets(w http.ResponseWriter, r *http.Request) {
	if err := s.store.Barrier(r.Context()); err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	st := s.store.Snapshot()
	names := model.SortedBucketNames(st.Buckets)
	result := listAllMyBucketsResult{Xmlns: "http://s3.amazonaws.com/doc/2006-03-01/"}
	for _, name := range names {
		b := st.Buckets[name]
		result.Buckets.Buckets = append(result.Buckets.Buckets, bucketXML{Name: b.Name, CreationDate: b.CreatedAt.UTC().Format(time.RFC3339)})
	}
	writeXML(w, http.StatusOK, result)
}

func (s *Server) handleListObjects(w http.ResponseWriter, r *http.Request, bucket string) {
	if err := s.store.Barrier(r.Context()); err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	if !s.store.BucketExists(bucket) {
		http.Error(w, "bucket not found", http.StatusNotFound)
		return
	}
	prefix := r.URL.Query().Get("prefix")
	delimiter := r.URL.Query().Get("delimiter")
	continuation := r.URL.Query().Get("continuation-token")
	maxKeys := 1000
	if raw := r.URL.Query().Get("max-keys"); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 {
			maxKeys = parsed
		}
	}
	st := s.store.Snapshot()
	var objs []*model.Object
	for _, obj := range st.Objects {
		if obj == nil || obj.Bucket != bucket {
			continue
		}
		if prefix != "" && !strings.HasPrefix(obj.Key, prefix) {
			continue
		}
		if continuation != "" && obj.Key <= continuation {
			continue
		}
		objs = append(objs, obj)
	}
	sort.Slice(objs, func(i, j int) bool { return objs[i].Key < objs[j].Key })

	result := listBucketResultV2{
		Xmlns:             "http://s3.amazonaws.com/doc/2006-03-01/",
		Name:              bucket,
		Prefix:            prefix,
		MaxKeys:           maxKeys,
		Delimiter:         delimiter,
		ContinuationToken: continuation,
	}
	prefixes := make(map[string]struct{})
	count := 0
	for _, obj := range objs {
		if delimiter != "" {
			trimmed := strings.TrimPrefix(obj.Key, prefix)
			if idx := strings.Index(trimmed, delimiter); idx >= 0 {
				cp := prefix + trimmed[:idx+len(delimiter)]
				if _, ok := prefixes[cp]; !ok {
					prefixes[cp] = struct{}{}
					result.CommonPrefixes = append(result.CommonPrefixes, commonPrefix{Prefix: cp})
					count++
				}
				if count >= maxKeys {
					result.IsTruncated = true
					result.NextContinuationToken = obj.Key
					break
				}
				continue
			}
		}
		result.Contents = append(result.Contents, objectContent{
			Key:          obj.Key,
			LastModified: obj.UpdatedAt.UTC().Format(time.RFC3339),
			ETag:         obj.ETag,
			Size:         obj.Size,
			StorageClass: storageClassForTier(obj.CurrentTier),
		})
		count++
		if count >= maxKeys {
			result.IsTruncated = true
			result.NextContinuationToken = obj.Key
			break
		}
	}
	result.KeyCount = len(result.Contents) + len(result.CommonPrefixes)
	writeXML(w, http.StatusOK, result)
}

func storageClassForTier(t model.Tier) string {
	switch t {
	case model.TierWarm:
		return "STANDARD_IA"
	default:
		return "STANDARD"
	}
}

func writeXML(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(code)
	_, _ = w.Write([]byte(xml.Header))
	enc := xml.NewEncoder(w)
	enc.Indent("", "  ")
	_ = enc.Encode(v)
}
