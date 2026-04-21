package storageclient

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
	"time"

	"tierstore/internal/model"
)

type Client struct {
	http *http.Client
}

func New(timeout time.Duration) *Client {
	return &Client{
		http: &http.Client{Timeout: timeout},
	}
}

func (c *Client) UploadFile(ctx context.Context, nodeURL string, tier model.Tier, objectID, filePath, sha256 string, size int64) error {
	f, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("open upload file: %w", err)
	}
	defer f.Close()

	target, err := internalObjectURL(nodeURL, tier, objectID)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, target, f)
	if err != nil {
		return fmt.Errorf("create upload request: %w", err)
	}
	req.ContentLength = size
	req.Header.Set("X-Checksum-SHA256", sha256)
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("upload to %s: %w", nodeURL, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<10))
		return fmt.Errorf("upload to %s failed: status=%d body=%q", nodeURL, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	if got := resp.Header.Get("X-Checksum-SHA256"); got != "" && got != sha256 {
		return fmt.Errorf("upload checksum mismatch from %s: got=%s want=%s", nodeURL, got, sha256)
	}
	return nil
}

func (c *Client) GetObject(ctx context.Context, nodeURL string, tier model.Tier, objectID string) (*http.Response, error) {
	target, err := internalObjectURL(nodeURL, tier, objectID)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return nil, fmt.Errorf("create get request: %w", err)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("get from %s: %w", nodeURL, err)
	}
	if resp.StatusCode/100 != 2 {
		defer resp.Body.Close()
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<10))
		return nil, fmt.Errorf("get from %s failed: status=%d body=%q", nodeURL, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return resp, nil
}

func (c *Client) HeadObject(ctx context.Context, nodeURL string, tier model.Tier, objectID string) (*http.Response, error) {
	target, err := internalObjectURL(nodeURL, tier, objectID)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, target, nil)
	if err != nil {
		return nil, fmt.Errorf("create head request: %w", err)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("head from %s: %w", nodeURL, err)
	}
	if resp.StatusCode/100 != 2 {
		defer resp.Body.Close()
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<10))
		return nil, fmt.Errorf("head from %s failed: status=%d body=%q", nodeURL, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return resp, nil
}

func (c *Client) DeleteObject(ctx context.Context, nodeURL string, tier model.Tier, objectID string) error {
	target, err := internalObjectURL(nodeURL, tier, objectID)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, target, nil)
	if err != nil {
		return fmt.Errorf("create delete request: %w", err)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("delete from %s: %w", nodeURL, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil
	}
	if resp.StatusCode/100 != 2 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<10))
		return fmt.Errorf("delete from %s failed: status=%d body=%q", nodeURL, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}

func internalObjectURL(nodeURL string, tier model.Tier, objectID string) (string, error) {
	u, err := url.Parse(nodeURL)
	if err != nil {
		return "", fmt.Errorf("parse node url %q: %w", nodeURL, err)
	}
	u.Path = path.Join(u.Path, "/_internal/tiers", string(tier), "objects", objectID)
	return u.String(), nil
}
