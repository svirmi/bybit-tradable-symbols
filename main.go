package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sort"
	"strings"
	"time"
)

// Constants for the Bybit API
const (
	bybitAPIEndpoint = "https://api.bybit.com/v5/market/instruments-info"
	requestTimeout   = 10 * time.Second
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

// Instrument represents a single symbol's data. We only need the Symbol and Status fields.
type Instrument struct {
	Symbol string `json:"symbol"`
	Status string `json:"status"`
}

// fetchSymbols retrieves all tradable symbols for a given market category (e.g., "spot" or "linear").
// It handles pagination to ensure all symbols are fetched.
// It sends the resulting symbol list through the provided channel.
func fetchSymbols(category string, ch chan<- []string) {
	var allSymbols []string
	cursor := ""
	client := &http.Client{
		Timeout: requestTimeout,
	}

	for {
		// The instruments-info endpoint is paginated. We loop using the cursor
		// until we have retrieved all pages of results.
		url := fmt.Sprintf("%s?category=%s&limit=1000", bybitAPIEndpoint, category)
		if cursor != "" {
			url = fmt.Sprintf("%s&cursor=%s", url, cursor)
		}

		fmt.Println(url)

		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			log.Printf("Error creating request for %s symbols: %v", category, err)
			ch <- nil // Send nil to signal an error
			return
		}

		req.Header.Set("Accept", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			log.Printf("Error fetching %s symbols: %v", category, err)
			ch <- nil
			return
		}

		if resp.StatusCode != http.StatusOK {
			log.Printf("Error response from API for %s symbols. Status: %s", category, resp.Status)
			ch <- nil
			return
		}

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			log.Printf("Error reading response body for %s symbols: %v", category, err)
			resp.Body.Close()
			ch <- nil
			return
		}
		resp.Body.Close()

		var apiResponse BybitResponse
		if err := json.Unmarshal(body, &apiResponse); err != nil {
			log.Printf("Error decoding JSON for %s symbols: %v", category, err)
			ch <- nil
			return
		}

		if apiResponse.RetCode != 0 {
			log.Printf("API returned an error for %s symbols: %s", category, apiResponse.RetMsg)
			ch <- nil
			return
		}

		// Extract the symbol strings from the instrument list for the current page
		for _, instrument := range apiResponse.Result.List {
			// Ensure we only include instruments that are actively trading.
			if instrument.Status == "Trading" {
				allSymbols = append(allSymbols, instrument.Symbol)
			}
		}

		// If the nextPageCursor is empty, we have reached the last page.
		if apiResponse.Result.NextPageCursor == "" {
			break
		}
		cursor = apiResponse.Result.NextPageCursor
	}

	fmt.Printf("Successfully fetched %d total symbols from the %s market.\n", len(allSymbols), category)
	ch <- allSymbols
}

// normalizeSymbol converts futures symbols to their spot equivalent for comparison.
// BTCPERP -> BTCUSDC (USDC perpetuals)
// BTCUSDT -> BTCUSDT (USDT perpetuals, unchanged)
// This handles the special case where USDC perpetuals use PERP suffix instead of USDC.
func normalizeSymbol(symbol string) string {
	if strings.HasSuffix(symbol, "PERP") {
		// Replace PERP suffix with USDC to match spot naming
		// But we need to ensure it's not PERPUSDT or similar where PERP is the base currency
		if len(symbol) > 4 {
			baseCurrency := symbol[:len(symbol)-4]
			// Additional check: make sure PERP is not part of the base currency name
			// If the symbol is exactly "PERP" + something, we don't convert it
			if baseCurrency != "PERP" && !strings.HasSuffix(baseCurrency, "PERP") {
				return baseCurrency + "USDC"
			}
		}
	}
	return symbol
}

// MatchResult holds the results of symbol matching between spot and futures.
type MatchResult struct {
	AllCommon  []string // All common symbols (USDT + USDC quoted)
	USDCQuoted []string // Only USDC-quoted symbols
}

// mergeAndFilter finds the common symbols between spot and futures markets.
// It handles the special case where USDC perpetuals have PERP suffix in futures
// but USDC suffix in spot markets.
// Returns both all common symbols and specifically USDC-quoted symbols.
func mergeAndFilter(spotSymbols, futuresSymbols []string) MatchResult {
	// Create a map of spot symbols for O(1) lookup
	spotMap := make(map[string]bool, len(spotSymbols))
	for _, symbol := range spotSymbols {
		spotMap[symbol] = true
	}

	// Track which futures symbols match (store original futures symbol)
	commonSymbolsMap := make(map[string]string) // normalized -> original futures symbol

	for _, futuresSymbol := range futuresSymbols {
		normalized := normalizeSymbol(futuresSymbol)

		// Check if the normalized futures symbol exists in spot
		if spotMap[normalized] {
			// Store with normalized key to avoid duplicates
			// The value is the original futures symbol for reference
			commonSymbolsMap[normalized] = futuresSymbol
		}
	}

	// Extract the normalized symbols (which match spot symbols)
	commonSymbols := make([]string, 0, len(commonSymbolsMap))
	usdcQuoted := make([]string, 0)

	for normalized := range commonSymbolsMap {
		commonSymbols = append(commonSymbols, normalized)

		// Check if this is a USDC-quoted pair
		if strings.HasSuffix(normalized, "USDC") {
			usdcQuoted = append(usdcQuoted, normalized)
		}
	}

	// Sort both lists for consistent and readable output.
	sort.Strings(commonSymbols)
	sort.Strings(usdcQuoted)

	return MatchResult{
		AllCommon:  commonSymbols,
		USDCQuoted: usdcQuoted,
	}
}

func main() {
	fmt.Println("Fetching tradable symbols from Bybit...")

	// Create channels to receive the results from the goroutines.
	spotSymbolsChan := make(chan []string)
	futuresSymbolsChan := make(chan []string)

	// Start two goroutines to fetch spot and futures symbols concurrently.
	// "linear" covers both USDT and USDC perpetuals/futures.
	go fetchSymbols("spot", spotSymbolsChan)
	go fetchSymbols("linear", futuresSymbolsChan)

	// Wait to receive the results from both channels. This blocks until both
	// goroutines have sent their data, effectively syncing the program.
	spotSymbols := <-spotSymbolsChan
	futuresSymbols := <-futuresSymbolsChan

	fmt.Println("--------------------------------------------------")

	// Check if either of the API calls failed.
	if spotSymbols == nil || futuresSymbols == nil {
		log.Fatal("Failed to fetch symbols from one or more markets. Exiting.")
		return
	}

	// Process the results to find the common symbols.
	result := mergeAndFilter(spotSymbols, futuresSymbols)

	// Print the final result.
	fmt.Printf("\nFound %d symbols present in BOTH Spot and Futures markets:\n\n", len(result.AllCommon))

	// Print in columns for better readability
	const columns = 5
	for i, symbol := range result.AllCommon {
		fmt.Printf("%-15s", symbol)
		if (i+1)%columns == 0 {
			fmt.Println()
		}
	}
	fmt.Println()

	// Print USDC-quoted pairs separately
	fmt.Println("\n--------------------------------------------------")
	fmt.Printf("\nFound %d USDC-quoted symbols (matched via PERP suffix):\n\n", len(result.USDCQuoted))

	for i, symbol := range result.USDCQuoted {
		fmt.Printf("%-15s", symbol)
		if (i+1)%columns == 0 {
			fmt.Println()
		}
	}
	fmt.Println() // Newline for clean exit
}
