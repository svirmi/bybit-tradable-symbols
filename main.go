package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"sync"
	"syscall"
	"time"
)

// Constants for the Bybit API and server configuration
const (
	bybitAPIEndpoint = "https://api.bybit.com/v5/market/instruments-info"
	requestTimeout   = 10 * time.Second
	updateInterval   = 2 * time.Minute
	serverPort       = "8080"
)

// BybitResponse defines the structure for the top-level API response.
type BybitResponse struct {
	RetCode int    `json:"retCode"`
	RetMsg  string `json:"retMsg"`
	Result  Result `json:"result"`
}

// Result contains the list of instruments from the API response and pagination info.
type Result struct {
	Category       string       `json:"category"`
	List           []Instrument `json:"list"`
	NextPageCursor string       `json:"nextPageCursor"`
}

// Instrument represents a single symbol's data with all relevant fields.
type Instrument struct {
	Symbol      string `json:"symbol"`
	Status      string `json:"status"`
	DisplayName string `json:"displayName"`
	SettleCoin  string `json:"settleCoin"`
	BaseCoin    string `json:"baseCoin"`
	QuoteCoin   string `json:"quoteCoin"`
}

// SymbolInfo holds processed information about a symbol.
type SymbolInfo struct {
	Symbol      string `json:"symbol"`
	DisplayName string `json:"displayName"`
	BaseCoin    string `json:"baseCoin"`
	QuoteCoin   string `json:"quoteCoin"`
	SettleCoin  string `json:"settleCoin"`
}

// SymbolResponse is the JSON response structure for the API.
type SymbolResponse struct {
	Timestamp string       `json:"timestamp"`
	Count     int          `json:"count"`
	Symbols   []SymbolInfo `json:"symbols"`
}

// SymbolCache holds the cached symbol data with thread-safe access.
//
// Concurrency design notes:
// - RWMutex allows multiple concurrent readers with exclusive writer access
// - Slice headers are copied during reads, but underlying arrays are shared (intentional)
// - This is safe because slices are never modified after creation (immutable pattern)
// - Updates create entirely new slices rather than modifying existing ones
// - JSON encoding happens after releasing the read lock, using the copied slice header
//
// Performance characteristics:
// - Read operations are extremely fast (O(1) slice header copy)
// - No memory allocation on reads (zero-copy for slice data)
// - Write operations (every 2 minutes) briefly block readers during pointer swap
//
// Potential improvements if needed:
// - For very high concurrency: implement deep copy on read (trades memory for isolation)
// - For zero-lock reads: use atomic.Value for lock-free pointer swapping
// - For large datasets: implement pagination to reduce response size
// - Current design prioritizes simplicity and read performance for the expected load
type SymbolCache struct {
	mu          sync.RWMutex
	allSymbols  []SymbolInfo
	usdtSymbols []SymbolInfo
	usdcSymbols []SymbolInfo
	lastUpdate  time.Time
}

var (
	cache  = &SymbolCache{}
	logger *slog.Logger
)

// fetchSymbols retrieves all tradable symbols for a given market category.
func fetchSymbols(ctx context.Context, category string) ([]SymbolInfo, error) {
	var allSymbols []SymbolInfo
	cursor := ""
	client := &http.Client{
		Timeout: requestTimeout,
	}

	for {
		url := fmt.Sprintf("%s?category=%s&limit=1000", bybitAPIEndpoint, category)
		if cursor != "" {
			url = fmt.Sprintf("%s&cursor=%s", url, cursor)
		}

		req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
		if err != nil {
			return nil, fmt.Errorf("creating request for %s: %w", category, err)
		}

		req.Header.Set("Accept", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("fetching %s symbols: %w", category, err)
		}

		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			return nil, fmt.Errorf("API returned status %s for %s", resp.Status, category)
		}

		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			return nil, fmt.Errorf("reading response body for %s: %w", category, err)
		}

		var apiResponse BybitResponse
		if err := json.Unmarshal(body, &apiResponse); err != nil {
			return nil, fmt.Errorf("decoding JSON for %s: %w", category, err)
		}

		if apiResponse.RetCode != 0 {
			return nil, fmt.Errorf("API error for %s: %s", category, apiResponse.RetMsg)
		}

		for _, instrument := range apiResponse.Result.List {
			if instrument.Status == "Trading" {
				displayName := instrument.DisplayName
				if displayName == "" {
					displayName = instrument.Symbol
				}

				allSymbols = append(allSymbols, SymbolInfo{
					Symbol:      instrument.Symbol,
					DisplayName: displayName,
					BaseCoin:    instrument.BaseCoin,
					QuoteCoin:   instrument.QuoteCoin,
					SettleCoin:  instrument.SettleCoin,
				})
			}
		}

		if apiResponse.Result.NextPageCursor == "" {
			break
		}
		cursor = apiResponse.Result.NextPageCursor
	}

	return allSymbols, nil
}

// updateSymbolCache fetches data from Bybit and updates the cache.
func updateSymbolCache(ctx context.Context) error {
	logger.Info("Starting symbol cache update")

	spotSymbols, err := fetchSymbols(ctx, "spot")
	if err != nil {
		return fmt.Errorf("fetching spot symbols: %w", err)
	}
	logger.Info("Fetched spot symbols", "count", len(spotSymbols))

	futuresSymbols, err := fetchSymbols(ctx, "linear")
	if err != nil {
		return fmt.Errorf("fetching futures symbols: %w", err)
	}
	logger.Info("Fetched futures symbols", "count", len(futuresSymbols))

	// Match symbols
	allCommon, usdtQuoted, usdcQuoted := matchSymbols(spotSymbols, futuresSymbols)

	// Update cache with exclusive write lock
	// Creates new slices entirely rather than modifying existing ones
	// Brief lock duration ensures minimal impact on concurrent readers
	cache.mu.Lock()
	cache.allSymbols = allCommon
	cache.usdtSymbols = usdtQuoted
	cache.usdcSymbols = usdcQuoted
	cache.lastUpdate = time.Now()
	cache.mu.Unlock()

	logger.Info("Cache updated successfully",
		"total", len(allCommon),
		"usdt", len(usdtQuoted),
		"usdc", len(usdcQuoted))

	return nil
}

// matchSymbols finds common symbols between spot and futures markets.
func matchSymbols(spotSymbols, futuresSymbols []SymbolInfo) (all, usdt, usdc []SymbolInfo) {
	spotMap := make(map[string]SymbolInfo)
	for _, symbol := range spotSymbols {
		spotMap[symbol.DisplayName] = symbol
	}

	matched := make(map[string]bool)

	for _, futuresSymbol := range futuresSymbols {
		if spotSymbol, exists := spotMap[futuresSymbol.DisplayName]; exists && !matched[futuresSymbol.DisplayName] {
			matched[futuresSymbol.DisplayName] = true

			info := SymbolInfo{
				Symbol:      spotSymbol.Symbol,
				DisplayName: futuresSymbol.DisplayName,
				BaseCoin:    futuresSymbol.BaseCoin,
				QuoteCoin:   futuresSymbol.QuoteCoin,
				SettleCoin:  futuresSymbol.SettleCoin,
			}

			all = append(all, info)

			if futuresSymbol.SettleCoin == "USDT" {
				usdt = append(usdt, info)
			} else if futuresSymbol.SettleCoin == "USDC" {
				usdc = append(usdc, info)
			}
		}
	}

	// Sort all slices by DisplayName
	sortByDisplayName := func(slice []SymbolInfo) {
		sort.Slice(slice, func(i, j int) bool {
			return slice[i].DisplayName < slice[j].DisplayName
		})
	}

	sortByDisplayName(all)
	sortByDisplayName(usdt)
	sortByDisplayName(usdc)

	return all, usdt, usdc
}

// symbolUpdateWorker runs in the background and updates the cache periodically.
func symbolUpdateWorker(ctx context.Context) {
	// Initial update
	if err := updateSymbolCache(ctx); err != nil {
		logger.Error("Initial cache update failed", "error", err)
	}

	ticker := time.NewTicker(updateInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Info("Update worker stopping")
			return
		case <-ticker.C:
			if err := updateSymbolCache(ctx); err != nil {
				logger.Error("Cache update failed", "error", err)
			}
		}
	}
}

// handleSymbols is the HTTP handler for symbol endpoints.
//
// Concurrency handling:
// - Each request runs in its own goroutine (handled by http.Server)
// - Cache reads use RLock allowing multiple concurrent readers
// - Slice header copy happens under lock, JSON encoding after lock release
// - This pattern is safe because we never modify slice contents after creation
func handleSymbols(symbolType string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		startTime := time.Now()

		// Only allow GET requests
		if r.Method != http.MethodGet {
			logger.Warn("Method not allowed",
				"method", r.Method,
				"path", r.URL.Path,
				"remote", r.RemoteAddr)
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		// Read from cache with shared lock (allows concurrent reads)
		// We copy the slice header (24 bytes) but share the underlying array
		// This is safe because slices are never modified after cache update
		cache.mu.RLock()
		var symbols []SymbolInfo
		timestamp := cache.lastUpdate

		switch symbolType {
		case "all":
			symbols = cache.allSymbols
		case "usdt":
			symbols = cache.usdtSymbols
		case "usdc":
			symbols = cache.usdcSymbols
		default:
			cache.mu.RUnlock()
			logger.Error("Invalid symbol type requested", "type", symbolType)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}
		cache.mu.RUnlock()

		// Check if cache is empty (no data yet)
		if timestamp.IsZero() {
			logger.Warn("Cache not initialized yet",
				"path", r.URL.Path,
				"remote", r.RemoteAddr)
			http.Error(w, "Data not available yet, please retry in a moment", http.StatusServiceUnavailable)
			return
		}

		response := SymbolResponse{
			Timestamp: timestamp.Format(time.RFC3339),
			Count:     len(symbols),
			Symbols:   symbols,
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(response); err != nil {
			logger.Error("Failed to encode response",
				"error", err,
				"path", r.URL.Path,
				"remote", r.RemoteAddr)
			return
		}

		logger.Info("Request served",
			"path", r.URL.Path,
			"type", symbolType,
			"count", len(symbols),
			"duration", time.Since(startTime),
			"remote", r.RemoteAddr)
	}
}

// healthHandler provides a health check endpoint.
func healthHandler(w http.ResponseWriter, r *http.Request) {
	cache.mu.RLock()
	lastUpdate := cache.lastUpdate
	symbolCount := len(cache.allSymbols)
	cache.mu.RUnlock()

	status := "healthy"
	statusCode := http.StatusOK

	if lastUpdate.IsZero() {
		status = "initializing"
		statusCode = http.StatusServiceUnavailable
	} else if time.Since(lastUpdate) > updateInterval*2 {
		status = "stale"
		statusCode = http.StatusServiceUnavailable
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)

	health := map[string]interface{}{
		"status":      status,
		"lastUpdate":  lastUpdate.Format(time.RFC3339),
		"symbolCount": symbolCount,
		"cacheAge":    time.Since(lastUpdate).String(),
	}

	json.NewEncoder(w).Encode(health)
}

func main() {
	// Initialize structured logger
	logger = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	logger.Info("Starting Bybit Symbol REST Server", "port", serverPort)

	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start background worker to update symbols
	go symbolUpdateWorker(ctx)

	// Set up HTTP routes
	mux := http.NewServeMux()
	mux.HandleFunc("/symbols/all", handleSymbols("all"))
	mux.HandleFunc("/symbols/usdt", handleSymbols("usdt"))
	mux.HandleFunc("/symbols/usdc", handleSymbols("usdc"))
	mux.HandleFunc("/health", healthHandler)

	// Create server
	server := &http.Server{
		Addr:         ":" + serverPort,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Start server in goroutine
	go func() {
		logger.Info("Server listening", "addr", server.Addr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("Server error", "error", err)
			os.Exit(1)
		}
	}()

	// Wait for interrupt signal for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	logger.Info("Shutting down server...")

	// Cancel background worker
	cancel()

	// Graceful shutdown with timeout
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("Server shutdown error", "error", err)
	}

	logger.Info("Server stopped")
}
