package node

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"tierstore/internal/model"
)

type Server struct {
	id      string
	hotDir  string
	warmDir string
}

func New(id, hotDir, warmDir string) *Server {
	return &Server{id: id, hotDir: hotDir, warmDir: warmDir}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/_internal/health", s.handleHealth)
	mux.HandleFunc("/_internal/tiers/", s.handleObject)
	return loggingMiddleware(mux)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"id":        s.id,
		"status":    "ok",
		"hot_dir":   s.hotDir,
		"warm_dir":  s.warmDir,
		"timestamp": time.Now().UTC(),
	})
}

func (s *Server) handleObject(w http.ResponseWriter, r *http.Request) {
	tier, objectID, ok := parseInternalObjectPath(r.URL.Path)
	if !ok {
		http.NotFound(w, r)
		return
	}
	objPath, err := s.objectPath(tier, objectID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodPut:
		s.handlePutObject(w, r, objPath)
	case http.MethodGet:
		s.handleGetObject(w, r, objPath)
	case http.MethodHead:
		s.handleHeadObject(w, r, objPath)
	case http.MethodDelete:
		s.handleDeleteObject(w, r, objPath)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handlePutObject(w http.ResponseWriter, r *http.Request, objPath string) {
	if err := os.MkdirAll(filepath.Dir(objPath), 0o755); err != nil {
		http.Error(w, fmt.Sprintf("mkdir: %v", err), http.StatusInternalServerError)
		return
	}
	tmp, err := os.CreateTemp(filepath.Dir(objPath), ".upload-*.tmp")
	if err != nil {
		http.Error(w, fmt.Sprintf("temp file: %v", err), http.StatusInternalServerError)
		return
	}
	tmpName := tmp.Name()
	defer func() {
		tmp.Close()
		_ = os.Remove(tmpName)
	}()

	h := sha256.New()
	written, err := io.Copy(io.MultiWriter(tmp, h), r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("write temp: %v", err), http.StatusInternalServerError)
		return
	}
	sum := hex.EncodeToString(h.Sum(nil))
	if want := r.Header.Get("X-Checksum-SHA256"); want != "" && !strings.EqualFold(want, sum) {
		http.Error(w, fmt.Sprintf("checksum mismatch: got=%s want=%s", sum, want), http.StatusBadRequest)
		return
	}
	if err := tmp.Sync(); err != nil {
		http.Error(w, fmt.Sprintf("sync temp: %v", err), http.StatusInternalServerError)
		return
	}
	if err := tmp.Close(); err != nil {
		http.Error(w, fmt.Sprintf("close temp: %v", err), http.StatusInternalServerError)
		return
	}
	if err := os.Rename(tmpName, objPath); err != nil {
		http.Error(w, fmt.Sprintf("rename temp: %v", err), http.StatusInternalServerError)
		return
	}
	w.Header().Set("X-Checksum-SHA256", sum)
	w.Header().Set("Content-Length", fmt.Sprintf("%d", written))
	w.WriteHeader(http.StatusCreated)
}

func (s *Server) handleGetObject(w http.ResponseWriter, r *http.Request, objPath string) {
	f, err := os.Open(objPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			http.NotFound(w, r)
			return
		}
		http.Error(w, fmt.Sprintf("open object: %v", err), http.StatusInternalServerError)
		return
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		http.Error(w, fmt.Sprintf("stat object: %v", err), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", fmt.Sprintf("%d", info.Size()))
	http.ServeContent(w, r, filepath.Base(objPath), info.ModTime(), f)
}

func (s *Server) handleHeadObject(w http.ResponseWriter, r *http.Request, objPath string) {
	info, err := os.Stat(objPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			http.NotFound(w, r)
			return
		}
		http.Error(w, fmt.Sprintf("stat object: %v", err), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", fmt.Sprintf("%d", info.Size()))
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleDeleteObject(w http.ResponseWriter, r *http.Request, objPath string) {
	err := os.Remove(objPath)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		http.Error(w, fmt.Sprintf("delete object: %v", err), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) objectPath(tier model.Tier, objectID string) (string, error) {
	if objectID == "" || strings.Contains(objectID, "/") {
		return "", fmt.Errorf("invalid object id")
	}
	root, err := s.tierRoot(tier)
	if err != nil {
		return "", err
	}
	prefix := sha256.Sum256([]byte(objectID))
	return filepath.Join(root, hex.EncodeToString(prefix[:1]), hex.EncodeToString(prefix[1:2]), objectID+".blob"), nil
}

func (s *Server) tierRoot(tier model.Tier) (string, error) {
	switch tier {
	case model.TierHot:
		return s.hotDir, nil
	case model.TierWarm:
		return s.warmDir, nil
	default:
		return "", fmt.Errorf("invalid tier %q", tier)
	}
}

func parseInternalObjectPath(p string) (model.Tier, string, bool) {
	const prefix = "/_internal/tiers/"
	if !strings.HasPrefix(p, prefix) {
		return "", "", false
	}
	rest := strings.TrimPrefix(p, prefix)
	parts := strings.Split(rest, "/")
	if len(parts) != 3 || parts[1] != "objects" {
		return "", "", false
	}
	tier := model.Tier(parts[0])
	if tier != model.TierHot && tier != model.TierWarm {
		return "", "", false
	}
	return tier, parts[2], true
}

func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(w, r)
	})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
