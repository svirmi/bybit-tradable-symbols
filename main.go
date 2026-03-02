package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
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
	bybitAPIEndpoint      = "https://api.bybit.com/v5/market/instruments-info"
	requestTimeout        = 10 * time.Second
	optionsUpdateInterval = 30 * time.Second
	serverPort            = "8080"

	// Options expiry window: 24-72 hours for optimal liquidity and reaction time
	minExpiryHours = 24
	maxExpiryHours = 72
)

// Supported base coins for options trading on Bybit
var optionBaseCoins = []string{"BTC", "ETH", "SOL", "XRP", "DOGE", "MNT"}

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
	Symbol       string `json:"symbol"`
	Status       string `json:"status"`
	DisplayName  string `json:"displayName"`
	SettleCoin   string `json:"settleCoin"`
	BaseCoin     string `json:"baseCoin"`
	QuoteCoin    string `json:"quoteCoin"`
	DeliveryTime string `json:"deliveryTime"` // For options expiry (unix timestamp in ms)
}

// SymbolInfo holds processed information about a symbol.
type SymbolInfo struct {
	Symbol      string `json:"symbol"`
	DisplayName string `json:"displayName"`
	BaseCoin    string `json:"baseCoin"`
	QuoteCoin   string `json:"quoteCoin"`
	SettleCoin  string `json:"settleCoin"`
	ExpiryDate  string `json:"expiryDate,omitempty"`  // ISO 8601 format, only for options
	StrikePrice string `json:"strikePrice,omitempty"` // Strike price, only for options
	OptionType  string `json:"optionType,omitempty"`  // "Call" or "Put", only for options
}

// SymbolResponse is the JSON response structure for the API.
type SymbolResponse struct {
	Timestamp string       `json:"timestamp"`
	Count     int          `json:"count"`
	Symbols   []SymbolInfo `json:"symbols"`
}

// OptionsCache holds the cached options data with thread-safe access.
type OptionsCache struct {
	mu            sync.RWMutex
	allOptions    []SymbolInfo
	optionsByBase map[string][]SymbolInfo // Key: base coin (BTC, ETH, etc.)
	lastUpdate    time.Time
}

var optionsCache = &OptionsCache{optionsByBase: make(map[string][]SymbolInfo)}

// fetchOptions retrieves options for a specific base coin, filtered by expiry window.
func fetchOptions(ctx context.Context, baseCoin string) ([]SymbolInfo, error) {
	var allOptions []SymbolInfo
	cursor := ""
	client := &http.Client{
		Timeout: requestTimeout,
	}

	now := time.Now()
	minExpiry := now.Add(time.Duration(minExpiryHours) * time.Hour)
	maxExpiry := now.Add(time.Duration(maxExpiryHours) * time.Hour)

	for {
		url := fmt.Sprintf("%s?category=option&baseCoin=%s&limit=1000", bybitAPIEndpoint, baseCoin)
		if cursor != "" {
			url = fmt.Sprintf("%s&cursor=%s", url, cursor)
		}

		req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
		if err != nil {
			return nil, fmt.Errorf("creating request for %s options: %w", baseCoin, err)
		}

		req.Header.Set("Accept", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("fetching %s options: %w", baseCoin, err)
		}

		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			return nil, fmt.Errorf("API returned status %s for %s options", resp.Status, baseCoin)
		}

		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			return nil, fmt.Errorf("reading response body for %s options: %w", baseCoin, err)
		}

		var apiResponse BybitResponse
		if err := json.Unmarshal(body, &apiResponse); err != nil {
			return nil, fmt.Errorf("decoding JSON for %s options: %w", baseCoin, err)
		}

		if apiResponse.RetCode != 0 {
			return nil, fmt.Errorf("API error for %s options: %s", baseCoin, apiResponse.RetMsg)
		}

		for _, instrument := range apiResponse.Result.List {
			// Parse expiry time from deliveryTime field (unix timestamp in milliseconds)
			if instrument.DeliveryTime == "" || instrument.DeliveryTime == "0" {
				continue
			}

			// Parse unix milliseconds
			var expiryMs int64
			if _, err := fmt.Sscanf(instrument.DeliveryTime, "%d", &expiryMs); err != nil {
				continue
			}
			deliveryTimeMs := time.UnixMilli(expiryMs)

			// Filter by expiry window (24-72 hours)
			if deliveryTimeMs.Before(minExpiry) || deliveryTimeMs.After(maxExpiry) {
				continue
			}

			displayName := instrument.DisplayName
			if displayName == "" {
				displayName = instrument.Symbol
			}

			// Parse strike price and option type from symbol
			// Format: BTC-25OCT25-67000-C or ETH-25OCT25-3500-P
			strikePrice, optionType := parseOptionSymbol(instrument.Symbol)

			allOptions = append(allOptions, SymbolInfo{
				Symbol:      instrument.Symbol,
				DisplayName: displayName,
				BaseCoin:    instrument.BaseCoin,
				QuoteCoin:   instrument.QuoteCoin,
				SettleCoin:  instrument.SettleCoin,
				ExpiryDate:  deliveryTimeMs.Format(time.RFC3339),
				StrikePrice: strikePrice,
				OptionType:  optionType,
			})
		}

		if apiResponse.Result.NextPageCursor == "" {
			break
		}
		cursor = apiResponse.Result.NextPageCursor
	}

	return allOptions, nil
}

// parseOptionSymbol extracts strike price and option type from option symbol.
// Symbol formats:
//   - BTC-25OCT25-67000-C (4 parts)
//   - XRP-27OCT25-2.15-C-USDT (5 parts with settlement coin)
//
// Returns: strikePrice, optionType ("Call" or "Put")
func parseOptionSymbol(symbol string) (string, string) {
	// Split by dash
	parts := make([]string, 0, 5)
	lastIdx := 0
	for i := 0; i < len(symbol); i++ {
		if symbol[i] == '-' {
			parts = append(parts, symbol[lastIdx:i])
			lastIdx = i + 1
		}
	}
	if lastIdx < len(symbol) {
		parts = append(parts, symbol[lastIdx:])
	}

	// Handle both formats:
	// 4 parts: [BASE, DATE, STRIKE, TYPE]
	// 5 parts: [BASE, DATE, STRIKE, TYPE, SETTLEMENT]
	if len(parts) < 4 || len(parts) > 5 {
		return "", ""
	}

	strikePrice := parts[2]
	optionTypeCode := parts[3]

	var optionType string
	switch optionTypeCode {
	case "C":
		optionType = "Call"
	case "P":
		optionType = "Put"
	default:
		optionType = ""
	}

	return strikePrice, optionType
}

// updateOptionsCache fetches options data from Bybit and updates the cache.
func updateOptionsCache(ctx context.Context) error {
	allOptions := make([]SymbolInfo, 0)
	optionsByBase := make(map[string][]SymbolInfo)

	// Fetch options for each supported base coin
	for _, baseCoin := range optionBaseCoins {
		options, err := fetchOptions(ctx, baseCoin)
		if err != nil {
			continue // Continue with other base coins even if one fails
		}

		allOptions = append(allOptions, options...)
		optionsByBase[baseCoin] = options
	}

	// Sort all options by expiry date
	sort.Slice(allOptions, func(i, j int) bool {
		return allOptions[i].ExpiryDate < allOptions[j].ExpiryDate
	})

	// Update cache
	optionsCache.mu.Lock()
	optionsCache.allOptions = allOptions
	optionsCache.optionsByBase = optionsByBase
	optionsCache.lastUpdate = time.Now()
	optionsCache.mu.Unlock()

	return nil
}

// optionsUpdateWorker runs in the background and updates options cache more frequently.
func optionsUpdateWorker(ctx context.Context) {
	// Initial update
	updateOptionsCache(ctx)

	ticker := time.NewTicker(optionsUpdateInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			updateOptionsCache(ctx)
		}
	}
}

// handleOptions is the HTTP handler for options endpoints.
func handleOptions(baseCoin string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Only allow GET requests
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		// Read from options cache
		optionsCache.mu.RLock()
		var options []SymbolInfo
		timestamp := optionsCache.lastUpdate

		if baseCoin == "all" {
			options = optionsCache.allOptions
		} else {
			options = optionsCache.optionsByBase[baseCoin]
		}
		optionsCache.mu.RUnlock()

		// Check if cache is empty
		if timestamp.IsZero() {
			http.Error(w, "Data not available yet, please retry in a moment", http.StatusServiceUnavailable)
			return
		}

		response := SymbolResponse{
			Timestamp: timestamp.Format(time.RFC3339),
			Count:     len(options),
			Symbols:   options,
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(response); err != nil {
			return
		}
	}
}

// healthHandler provides a health check endpoint.
func healthHandler(w http.ResponseWriter, r *http.Request) {
	optionsCache.mu.RLock()
	optionsLastUpdate := optionsCache.lastUpdate
	optionsCount := len(optionsCache.allOptions)
	optionsCache.mu.RUnlock()

	status := "healthy"
	statusCode := http.StatusOK

	if optionsLastUpdate.IsZero() {
		status = "initializing"
		statusCode = http.StatusServiceUnavailable
	} else if time.Since(optionsLastUpdate) > optionsUpdateInterval*2 {
		status = "options_stale"
		statusCode = http.StatusServiceUnavailable
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)

	health := map[string]interface{}{
		"status":            status,
		"optionsLastUpdate": optionsLastUpdate.Format(time.RFC3339),
		"optionsCount":      optionsCount,
		"optionsCacheAge":   time.Since(optionsLastUpdate).String(),
	}

	json.NewEncoder(w).Encode(health)
}

func main() {
	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start background worker to update options
	go optionsUpdateWorker(ctx)

	// Set up HTTP routes
	mux := http.NewServeMux()

	// Options endpoints
	mux.HandleFunc("/options/all", handleOptions("all"))
	mux.HandleFunc("/options/btc", handleOptions("BTC"))
	mux.HandleFunc("/options/eth", handleOptions("ETH"))
	mux.HandleFunc("/options/sol", handleOptions("SOL"))
	mux.HandleFunc("/options/xrp", handleOptions("XRP"))
	mux.HandleFunc("/options/doge", handleOptions("DOGE"))
	mux.HandleFunc("/options/mnt", handleOptions("MNT"))

	// Health check
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
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			os.Exit(1)
		}
	}()

	// Wait for interrupt signal for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	// Cancel background workers
	cancel()

	// Graceful shutdown with timeout
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	server.Shutdown(shutdownCtx)
}
