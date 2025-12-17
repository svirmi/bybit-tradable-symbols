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

const (
	bybitAPIEndpoint = "https://api.bybit.com/v5/market/instruments-info"
	requestTimeout   = 10 * time.Second
	serverPort       = "8080"
	updateInterval   = 2 * time.Minute
)

// Instrument represents a symbol from Bybit with all relevant fields
type Instrument struct {
	Symbol        string `json:"symbol"`
	Status        string `json:"status"`
	DisplayName   string `json:"displayName"`
	SettleCoin    string `json:"settleCoin"`
	BaseCoin      string `json:"baseCoin"`
	QuoteCoin     string `json:"quoteCoin"`
	LotSizeFilter struct {
		MinOrderQty string `json:"minOrderQty"`
	} `json:"lotSizeFilter"`
}

// USDCUSDTPair represents a symbol pair with both USDC and USDT versions
type USDCUSDTPair struct {
	BaseSymbol string `json:"baseSymbol"`
	USDC       struct {
		Symbol      string `json:"symbol"`
		DisplayName string `json:"displayName"`
		MinOrderQty string `json:"minOrderQty"`
	} `json:"usdc"`
	USDT struct {
		Symbol      string `json:"symbol"`
		DisplayName string `json:"displayName"`
		MinOrderQty string `json:"minOrderQty"`
	} `json:"usdt"`
}

// USDCUSDTResponse is the JSON response structure
type USDCUSDTResponse struct {
	Timestamp string         `json:"timestamp"`
	Count     int            `json:"count"`
	Symbols   []USDCUSDTPair `json:"symbols"`
}

// Cache holds the cached symbol data with thread-safe access
type Cache struct {
	mu         sync.RWMutex
	pairs      []USDCUSDTPair
	lastUpdate time.Time
}

var (
	cache = &Cache{}
)

// fetchSymbols retrieves all tradable symbols for a given market category
func fetchSymbols(ctx context.Context, category string) ([]Instrument, error) {
	var allSymbols []Instrument
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
			return nil, fmt.Errorf("creating request: %w", err)
		}

		req.Header.Set("Accept", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("fetching symbols: %w", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("API returned status %s", resp.Status)
		}

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("reading response body: %w", err)
		}

		var apiResponse struct {
			RetCode int    `json:"retCode"`
			RetMsg  string `json:"retMsg"`
			Result  struct {
				List           []Instrument `json:"list"`
				NextPageCursor string       `json:"nextPageCursor"`
			} `json:"result"`
		}

		if err := json.Unmarshal(body, &apiResponse); err != nil {
			return nil, fmt.Errorf("decoding JSON: %w", err)
		}

		if apiResponse.RetCode != 0 {
			return nil, fmt.Errorf("API error: %s", apiResponse.RetMsg)
		}

		allSymbols = append(allSymbols, apiResponse.Result.List...)

		if apiResponse.Result.NextPageCursor == "" {
			break
		}
		cursor = apiResponse.Result.NextPageCursor
	}

	return allSymbols, nil
}

// findUSDCUSDTPairs finds symbols that have both USDC and USDT versions
func findUSDCUSDTPairs() ([]USDCUSDTPair, error) {
	ctx := context.Background()

	// Fetch all linear symbols
	futuresSymbols, err := fetchSymbols(ctx, "linear")
	if err != nil {
		return nil, fmt.Errorf("fetching futures symbols: %w", err)
	}

	// Group symbols by base coin (remove currency suffix)
	symbolGroups := make(map[string][]Instrument)
	for _, symbol := range futuresSymbols {
		baseSymbol := symbol.Symbol

		if len(baseSymbol) > 4 {
			if baseSymbol[len(baseSymbol)-4:] == "USDT" {
				baseSymbol = baseSymbol[:len(baseSymbol)-4]
			} else if baseSymbol[len(baseSymbol)-4:] == "PERP" {
				baseSymbol = baseSymbol[:len(baseSymbol)-4]
			}
		}

		symbolGroups[baseSymbol] = append(symbolGroups[baseSymbol], symbol)
	}

	var pairs []USDCUSDTPair

	// Find base coins that have both USDC and USDT versions
	for baseSymbol, symbols := range symbolGroups {
		var usdcSymbol, usdtSymbol Instrument
		hasUSDC, hasUSDT := false, false

		for _, symbol := range symbols {
			if symbol.SettleCoin == "USDC" {
				usdcSymbol = symbol
				hasUSDC = true
			} else if symbol.SettleCoin == "USDT" {
				usdtSymbol = symbol
				hasUSDT = true
			}
		}

		if hasUSDC && hasUSDT {
			pair := USDCUSDTPair{
				BaseSymbol: baseSymbol,
				USDC: struct {
					Symbol      string `json:"symbol"`
					DisplayName string `json:"displayName"`
					MinOrderQty string `json:"minOrderQty"`
				}{
					Symbol:      usdcSymbol.Symbol,
					DisplayName: usdcSymbol.DisplayName,
					MinOrderQty: usdcSymbol.LotSizeFilter.MinOrderQty,
				},
				USDT: struct {
					Symbol      string `json:"symbol"`
					DisplayName string `json:"displayName"`
					MinOrderQty string `json:"minOrderQty"`
				}{
					Symbol:      usdtSymbol.Symbol,
					DisplayName: usdtSymbol.DisplayName,
					MinOrderQty: usdtSymbol.LotSizeFilter.MinOrderQty,
				},
			}
			pairs = append(pairs, pair)
		}
	}

	sort.Slice(pairs, func(i, j int) bool {
		return pairs[i].BaseSymbol < pairs[j].BaseSymbol
	})

	return pairs, nil
}

// updateCache updates the cache with fresh symbol data
func updateCache() error {
	pairs, err := findUSDCUSDTPairs()
	if err != nil {
		return fmt.Errorf("finding USDC/USDT pairs: %w", err)
	}

	cache.mu.Lock()
	defer cache.mu.Unlock()
	cache.pairs = pairs
	cache.lastUpdate = time.Now()

	return nil
}

// cacheUpdateWorker updates the cache periodically
func cacheUpdateWorker(ctx context.Context) {
	if err := updateCache(); err != nil {
		fmt.Printf("Initial cache update failed: %v\n", err)
	}

	ticker := time.NewTicker(updateInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := updateCache(); err != nil {
				fmt.Printf("Cache update failed: %v\n", err)
			}
		}
	}
}

// handleUSDCUSDT handles HTTP requests for USDC/USDT symbol pairs
func handleUSDCUSDT(w http.ResponseWriter, r *http.Request) {
	cache.mu.RLock()
	defer cache.mu.RUnlock()

	if cache.lastUpdate.IsZero() {
		http.Error(w, "Data not available yet", http.StatusServiceUnavailable)
		return
	}

	response := USDCUSDTResponse{
		Timestamp: cache.lastUpdate.Format(time.RFC3339),
		Count:     len(cache.pairs),
		Symbols:   cache.pairs,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// main entry point for the application
func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go cacheUpdateWorker(ctx)

	mux := http.NewServeMux()
	mux.HandleFunc("/symbols/bybit-usdc-usdt", handleUSDCUSDT)

	server := &http.Server{
		Addr:         ":" + serverPort,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	go func() {
		fmt.Printf("Server listening on port %s\n", serverPort)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Printf("Server error: %v\n", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	fmt.Println("Shutting down server...")
	server.Shutdown(ctx)
}
