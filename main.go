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
	"path/filepath"
	"sort"
	"sync"
	"syscall"
	"time"
)

// Constants for the Bybit API and server configuration
const (
	bybitAPIEndpoint      = "https://api.bybit.com/v5/market/instruments-info"
	binanceAPIEndpoint    = "https://fapi.binance.com/fapi/v1/exchangeInfo"
	requestTimeout        = 10 * time.Second
	updateInterval        = 2 * time.Minute
	optionsUpdateInterval = 30 * time.Second
	serverPort            = "8080"
	logsDir               = "./logs"
	logRetentionDays      = 7

	// Options expiry window: 24-72 hours for optimal liquidity and reaction time
	minExpiryHours = 24
	maxExpiryHours = 72
)

// Supported base coins for options trading on Bybit
var optionBaseCoins = []string{"BTC", "ETH", "SOL", "XRP", "DOGE", "MNT"}

// BinanceExchangeInfo defines the structure for Binance API response.
type BinanceExchangeInfo struct {
	Symbols []BinanceSymbol `json:"symbols"`
}

// BinanceSymbol represents a symbol from Binance.
type BinanceSymbol struct {
	Symbol         string `json:"symbol"`
	Status         string `json:"status"`
	BaseAsset      string `json:"baseAsset"`
	QuoteAsset     string `json:"quoteAsset"`
	ContractType   string `json:"contractType"`
	ContractStatus string `json:"contractStatus"`
}

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
	Status      string `json:"status"`
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

// IntersectionSymbol represents a symbol that exists on both exchanges.
type IntersectionSymbol struct {
	Symbol    string `json:"symbol"`
	BaseCoin  string `json:"baseCoin"`
	OnBinance bool   `json:"onBinance"`
	OnBybit   bool   `json:"onBybit"`
}

// IntersectionResponse is the JSON response for intersection endpoint.
type IntersectionResponse struct {
	Timestamp    string               `json:"timestamp"`
	Count        int                  `json:"count"`
	BinanceCount int                  `json:"binanceCount"`
	BybitCount   int                  `json:"bybitCount"`
	Symbols      []IntersectionSymbol `json:"symbols"`
}

// SymbolCache holds the cached symbol data with thread-safe access.
type SymbolCache struct {
	mu          sync.RWMutex
	allSymbols  []SymbolInfo
	usdtSymbols []SymbolInfo
	usdcSymbols []SymbolInfo
	lastUpdate  time.Time
}

// OptionsCache holds the cached options data with thread-safe access.
type OptionsCache struct {
	mu            sync.RWMutex
	allOptions    []SymbolInfo
	optionsByBase map[string][]SymbolInfo // Key: base coin (BTC, ETH, etc.)
	lastUpdate    time.Time
}

// IntersectionCache holds the cached intersection data with thread-safe access.
type IntersectionCache struct {
	mu             sync.RWMutex
	commonSymbols  []IntersectionSymbol
	binanceSymbols []string
	bybitSymbols   []string
	lastUpdate     time.Time
}

var (
	cache             = &SymbolCache{}
	optionsCache      = &OptionsCache{optionsByBase: make(map[string][]SymbolInfo)}
	intersectionCache = &IntersectionCache{}
	logger            *slog.Logger
	logFile           *os.File
	logCloser         = make(chan struct{})
	logCloseOnce      sync.Once
)

// fetchSymbols retrieves all tradable symbols for a given market category from ByBit.
func fetchByBit(ctx context.Context, category string) ([]SymbolInfo, error) {
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
				Status:      instrument.Status,
			})
		}

		if apiResponse.Result.NextPageCursor == "" {
			break
		}
		cursor = apiResponse.Result.NextPageCursor
	}

	return allSymbols, nil
}

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

			// Debug log if parsing failed
			if strikePrice == "" || optionType == "" {
				logger.Debug("Failed to parse option symbol",
					"symbol", instrument.Symbol,
					"strikePrice", strikePrice,
					"optionType", optionType)
			}

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

// fetchBinanceFutures retrieves USDT-M perpetual futures from Binance.
func fetchBinanceFutures(ctx context.Context) ([]string, error) {
	client := &http.Client{
		Timeout: requestTimeout,
	}

	req, err := http.NewRequestWithContext(ctx, "GET", binanceAPIEndpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("creating Binance request: %w", err)
	}

	req.Header.Set("Accept", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetching Binance futures: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Binance API returned status %s", resp.Status)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading Binance response: %w", err)
	}

	var exchangeInfo BinanceExchangeInfo
	if err := json.Unmarshal(body, &exchangeInfo); err != nil {
		return nil, fmt.Errorf("decoding Binance JSON: %w", err)
	}

	var perpetuals []string
	for _, symbol := range exchangeInfo.Symbols {

		// Filter: PERPETUAL contracts with TRADING status and USDT quote
		if symbol.Status == "TRADING" && symbol.QuoteAsset == "USDT" && symbol.ContractType == "PERPETUAL" {
			perpetuals = append(perpetuals, symbol.Symbol)
		}
	}

	return perpetuals, nil
}

// fetchBybitFutures retrieves USDT perpetual futures from Bybit.
func fetchBybitFutures(ctx context.Context) ([]string, error) {
	futuresSymbols, err := fetchByBit(ctx, "linear")
	if err != nil {
		return nil, err
	}

	var usdtPerpetuals []string
	for _, symbol := range futuresSymbols {
		// Only USDT-settled perpetuals
		if symbol.Status == "Trading" && symbol.SettleCoin == "USDT" {
			usdtPerpetuals = append(usdtPerpetuals, symbol.Symbol)
		}
	}

	return usdtPerpetuals, nil
}

// updateIntersectionCache fetches data from both exchanges and finds common symbols.
func updateIntersectionCache(ctx context.Context) error {
	logger.Info("Starting intersection cache update")

	// Fetch from both exchanges
	binanceSymbols, err := fetchBinanceFutures(ctx)
	if err != nil {
		return fmt.Errorf("fetching Binance futures: %w", err)
	}
	logger.Info("Fetched Binance USDT perpetuals", "count", len(binanceSymbols))

	bybitSymbols, err := fetchBybitFutures(ctx)
	if err != nil {
		return fmt.Errorf("fetching Bybit futures: %w", err)
	}
	logger.Info("Fetched Bybit USDT perpetuals", "count", len(bybitSymbols))

	// Find intersection
	commonSymbols := findIntersection(binanceSymbols, bybitSymbols)

	// Update cache
	intersectionCache.mu.Lock()
	intersectionCache.commonSymbols = commonSymbols
	intersectionCache.binanceSymbols = binanceSymbols
	intersectionCache.bybitSymbols = bybitSymbols
	intersectionCache.lastUpdate = time.Now()
	intersectionCache.mu.Unlock()

	logger.Info("Intersection cache updated",
		"common", len(commonSymbols),
		"binance", len(binanceSymbols),
		"bybit", len(bybitSymbols))

	return nil
}

// findIntersection finds symbols that exist on both exchanges.
func findIntersection(binanceSymbols, bybitSymbols []string) []IntersectionSymbol {
	// Create maps for O(1) lookup
	binanceMap := make(map[string]bool)
	for _, symbol := range binanceSymbols {
		binanceMap[symbol] = true
	}

	bybitMap := make(map[string]bool)
	for _, symbol := range bybitSymbols {
		bybitMap[symbol] = true
	}

	// Find common symbols
	commonMap := make(map[string]bool)
	for symbol := range binanceMap {
		if bybitMap[symbol] {
			commonMap[symbol] = true
		}
	}

	// Convert to slice and extract base coin
	var result []IntersectionSymbol
	for symbol := range commonMap {
		// Extract base coin (remove USDT suffix)
		baseCoin := symbol
		if len(symbol) > 4 && symbol[len(symbol)-4:] == "USDT" {
			baseCoin = symbol[:len(symbol)-4]
		}

		result = append(result, IntersectionSymbol{
			Symbol:    symbol,
			BaseCoin:  baseCoin,
			OnBinance: true,
			OnBybit:   true,
		})
	}

	// Sort by symbol name
	sort.Slice(result, func(i, j int) bool {
		return result[i].Symbol < result[j].Symbol
	})

	return result
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

// updateSymbolCache fetches data from Bybit and updates the cache.
func updateSymbolCache(ctx context.Context) error {
	logger.Info("Starting symbol cache update")

	spotSymbols, err := fetchByBit(ctx, "spot")
	if err != nil {
		return fmt.Errorf("fetching spot symbols: %w", err)
	}
	logger.Info("Fetched spot symbols", "count", len(spotSymbols))

	futuresSymbols, err := fetchByBit(ctx, "linear")
	if err != nil {
		return fmt.Errorf("fetching futures symbols: %w", err)
	}
	logger.Info("Fetched ByBit futures symbols", "count", len(futuresSymbols))

	// Match symbols
	allCommon, usdtQuoted, usdcQuoted := matchSymbols(spotSymbols, futuresSymbols)

	// Update cache with exclusive write lock
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

// updateOptionsCache fetches options data from Bybit and updates the cache.
func updateOptionsCache(ctx context.Context) error {
	logger.Info("Starting options cache update")

	allOptions := make([]SymbolInfo, 0)
	optionsByBase := make(map[string][]SymbolInfo)

	// Fetch options for each supported base coin
	for _, baseCoin := range optionBaseCoins {
		options, err := fetchOptions(ctx, baseCoin)
		if err != nil {
			logger.Error("Failed to fetch options", "baseCoin", baseCoin, "error", err)
			continue // Continue with other base coins even if one fails
		}

		logger.Info("Fetched options", "baseCoin", baseCoin, "count", len(options))

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

	logger.Info("Options cache updated successfully", "total", len(allOptions))
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

// intersectionUpdateWorker runs in the background and updates intersection cache.
func intersectionUpdateWorker(ctx context.Context) {
	// Initial update
	if err := updateIntersectionCache(ctx); err != nil {
		logger.Error("Initial intersection cache update failed", "error", err)
	}

	ticker := time.NewTicker(updateInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Info("Intersection update worker stopping")
			return
		case <-ticker.C:
			if err := updateIntersectionCache(ctx); err != nil {
				logger.Error("Intersection cache update failed", "error", err)
			}
		}
	}
}

// optionsUpdateWorker runs in the background and updates options cache more frequently.
func optionsUpdateWorker(ctx context.Context) {
	// Initial update
	if err := updateOptionsCache(ctx); err != nil {
		logger.Error("Initial options cache update failed", "error", err)
	}

	ticker := time.NewTicker(optionsUpdateInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Info("Options update worker stopping")
			return
		case <-ticker.C:
			if err := updateOptionsCache(ctx); err != nil {
				logger.Error("Options cache update failed", "error", err)
			}
		}
	}
}

// handleSymbols is the HTTP handler for symbol endpoints.
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

		// Read from cache with shared lock
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

		// Check if cache is empty
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

// handleIntersection is the HTTP handler for intersection endpoint.
func handleIntersection(w http.ResponseWriter, r *http.Request) {
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

	// Read from intersection cache
	intersectionCache.mu.RLock()
	commonSymbols := intersectionCache.commonSymbols
	binanceCount := len(intersectionCache.binanceSymbols)
	bybitCount := len(intersectionCache.bybitSymbols)
	timestamp := intersectionCache.lastUpdate
	intersectionCache.mu.RUnlock()

	// Check if cache is empty
	if timestamp.IsZero() {
		logger.Warn("Intersection cache not initialized yet",
			"path", r.URL.Path,
			"remote", r.RemoteAddr)
		http.Error(w, "Data not available yet, please retry in a moment", http.StatusServiceUnavailable)
		return
	}

	response := IntersectionResponse{
		Timestamp:    timestamp.Format(time.RFC3339),
		Count:        len(commonSymbols),
		BinanceCount: binanceCount,
		BybitCount:   bybitCount,
		Symbols:      commonSymbols,
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		logger.Error("Failed to encode response",
			"error", err,
			"path", r.URL.Path,
			"remote", r.RemoteAddr)
		return
	}

	logger.Info("Intersection request served",
		"path", r.URL.Path,
		"count", len(commonSymbols),
		"duration", time.Since(startTime),
		"remote", r.RemoteAddr)
}

// handleOptions is the HTTP handler for options endpoints.
func handleOptions(baseCoin string) http.HandlerFunc {
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
			logger.Warn("Options cache not initialized yet",
				"path", r.URL.Path,
				"remote", r.RemoteAddr)
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
			logger.Error("Failed to encode response",
				"error", err,
				"path", r.URL.Path,
				"remote", r.RemoteAddr)
			return
		}

		logger.Info("Options request served",
			"path", r.URL.Path,
			"baseCoin", baseCoin,
			"count", len(options),
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

	optionsCache.mu.RLock()
	optionsLastUpdate := optionsCache.lastUpdate
	optionsCount := len(optionsCache.allOptions)
	optionsCache.mu.RUnlock()

	intersectionCache.mu.RLock()
	intersectionLastUpdate := intersectionCache.lastUpdate
	intersectionCount := len(intersectionCache.commonSymbols)
	intersectionCache.mu.RUnlock()

	status := "healthy"
	statusCode := http.StatusOK

	if lastUpdate.IsZero() || optionsLastUpdate.IsZero() || intersectionLastUpdate.IsZero() {
		status = "initializing"
		statusCode = http.StatusServiceUnavailable
	} else if time.Since(lastUpdate) > updateInterval*2 {
		status = "stale"
		statusCode = http.StatusServiceUnavailable
	} else if time.Since(optionsLastUpdate) > optionsUpdateInterval*2 {
		status = "options_stale"
		statusCode = http.StatusServiceUnavailable
	} else if time.Since(intersectionLastUpdate) > updateInterval*2 {
		status = "intersection_stale"
		statusCode = http.StatusServiceUnavailable
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)

	health := map[string]interface{}{
		"status":                 status,
		"lastUpdate":             lastUpdate.Format(time.RFC3339),
		"symbolCount":            symbolCount,
		"cacheAge":               time.Since(lastUpdate).String(),
		"optionsLastUpdate":      optionsLastUpdate.Format(time.RFC3339),
		"optionsCount":           optionsCount,
		"optionsCacheAge":        time.Since(optionsLastUpdate).String(),
		"intersectionLastUpdate": intersectionLastUpdate.Format(time.RFC3339),
		"intersectionCount":      intersectionCount,
		"intersectionCacheAge":   time.Since(intersectionLastUpdate).String(),
	}

	json.NewEncoder(w).Encode(health)
}

// setupLogging configures logging to both console and daily rotating files.
func setupLogging() (func(), error) {
	// Create logs directory if it doesn't exist
	if err := os.MkdirAll(logsDir, 0755); err != nil {
		return nil, fmt.Errorf("creating logs directory: %w", err)
	}

	// Create log file with current date
	logFileName := filepath.Join(logsDir, fmt.Sprintf("server-%s.log", time.Now().Format("2006-01-02")))
	file, err := os.OpenFile(logFileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("opening log file: %w", err)
	}
	logFile = file

	// Create multi-writer for both console and file
	multiWriter := io.MultiWriter(os.Stdout, file)

	// Create logger with multi-writer
	logger = slog.New(slog.NewJSONHandler(multiWriter, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	// Start background worker for log rotation and cleanup
	go logRotationWorker()

	// Return cleanup function
	cleanup := func() {
		logCloseOnce.Do(func() {
			close(logCloser)
			if logFile != nil {
				logFile.Close()
			}
		})
	}

	return cleanup, nil
}

// logRotationWorker handles daily log rotation and cleanup of old logs.
func logRotationWorker() {
	// Calculate time until midnight for first rotation
	now := time.Now()
	tomorrow := time.Date(now.Year(), now.Month(), now.Day()+1, 0, 0, 0, 0, now.Location())
	timeUntilMidnight := tomorrow.Sub(now)

	// Wait until midnight for first rotation
	timer := time.NewTimer(timeUntilMidnight)
	defer timer.Stop()

	for {
		select {
		case <-logCloser:
			return
		case <-timer.C:
			// Rotate log file
			if err := rotateLogs(); err != nil {
				if logger != nil {
					logger.Error("Failed to rotate logs", "error", err)
				}
			}

			// Clean up old log files
			if err := cleanupOldLogs(); err != nil {
				if logger != nil {
					logger.Error("Failed to cleanup old logs", "error", err)
				}
			}

			// Reset timer for next midnight (24 hours)
			timer.Reset(24 * time.Hour)
		}
	}
}

// rotateLogs creates a new log file for the current day.
func rotateLogs() error {
	// Create new log file with current date
	logFileName := filepath.Join(logsDir, fmt.Sprintf("server-%s.log", time.Now().Format("2006-01-02")))
	newFile, err := os.OpenFile(logFileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("creating new log file: %w", err)
	}

	// Close old file
	if logFile != nil {
		logFile.Close()
	}

	// Update global file reference
	logFile = newFile

	// Update logger with new multi-writer
	multiWriter := io.MultiWriter(os.Stdout, newFile)
	logger = slog.New(slog.NewJSONHandler(multiWriter, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	logger.Info("Log file rotated", "file", logFileName)
	return nil
}

// cleanupOldLogs removes log files older than the retention period.
func cleanupOldLogs() error {
	entries, err := os.ReadDir(logsDir)
	if err != nil {
		return fmt.Errorf("reading logs directory: %w", err)
	}

	cutoffDate := time.Now().AddDate(0, 0, -logRetentionDays)
	deletedCount := 0

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		// Only process files matching our log pattern
		matched, err := filepath.Match("server-*.log", entry.Name())
		if err != nil || !matched {
			continue
		}

		info, err := entry.Info()
		if err != nil {
			logger.Warn("Failed to get file info", "file", entry.Name(), "error", err)
			continue
		}

		// Delete if older than retention period
		if info.ModTime().Before(cutoffDate) {
			filePath := filepath.Join(logsDir, entry.Name())
			if err := os.Remove(filePath); err != nil {
				logger.Warn("Failed to delete old log file", "file", entry.Name(), "error", err)
			} else {
				deletedCount++
				logger.Info("Deleted old log file", "file", entry.Name())
			}
		}
	}

	if deletedCount > 0 {
		logger.Info("Log cleanup completed", "deleted", deletedCount)
	}

	return nil
}

func main() {
	// Initialize logging to both console and file
	cleanupLogs, err := setupLogging()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to setup logging: %v\n", err)
		os.Exit(1)
	}
	defer cleanupLogs()

	logger.Info("Starting Bybit Symbol REST Server", "port", serverPort)

	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start background worker to update symbols
	go symbolUpdateWorker(ctx)

	// Start background worker to update options (faster interval)
	go optionsUpdateWorker(ctx)

	// Start background worker to update intersection
	go intersectionUpdateWorker(ctx)

	// Set up HTTP routes
	mux := http.NewServeMux()

	// Spot/Futures endpoints
	mux.HandleFunc("/symbols/all", handleSymbols("all"))
	mux.HandleFunc("/symbols/usdt", handleSymbols("usdt"))
	mux.HandleFunc("/symbols/usdc", handleSymbols("usdc"))

	// Options endpoints
	mux.HandleFunc("/options/all", handleOptions("all"))
	mux.HandleFunc("/options/btc", handleOptions("BTC"))
	mux.HandleFunc("/options/eth", handleOptions("ETH"))
	mux.HandleFunc("/options/sol", handleOptions("SOL"))
	mux.HandleFunc("/options/xrp", handleOptions("XRP"))
	mux.HandleFunc("/options/doge", handleOptions("DOGE"))
	mux.HandleFunc("/options/mnt", handleOptions("MNT"))

	// Intersection endpoint
	mux.HandleFunc("/intersection/futures", handleIntersection)

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

	// Cancel background workers
	cancel()

	// Graceful shutdown with timeout
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("Server shutdown error", "error", err)
	}

	logger.Info("Server stopped")
}
