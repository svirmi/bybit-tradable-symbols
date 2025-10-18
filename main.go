package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sort"
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

// Instrument represents a single symbol's data with all relevant fields.
type Instrument struct {
	Symbol      string `json:"symbol"`
	Status      string `json:"status"`
	DisplayName string `json:"displayName"` // Used for matching (e.g., "AEVOUSDC", "AI16ZUSDC")
	SettleCoin  string `json:"settleCoin"`  // "USDT" or "USDC"
	BaseCoin    string `json:"baseCoin"`    // Base currency (e.g., "BTC", "ETH")
	QuoteCoin   string `json:"quoteCoin"`   // Quote currency for futures
}

// SymbolInfo holds processed information about a symbol.
type SymbolInfo struct {
	Symbol      string
	DisplayName string
	SettleCoin  string
	BaseCoin    string
}

// MatchResult holds the results of symbol matching between spot and futures.
type MatchResult struct {
	AllCommon     []SymbolInfo // All common symbols (USDT + USDC quoted)
	USDTQuoted    []SymbolInfo // Only USDT-quoted symbols
	USDCQuoted    []SymbolInfo // Only USDC-quoted symbols
	USDCPerpMatch []string     // USDC symbols matched via PERP suffix
}

// fetchSymbols retrieves all tradable symbols for a given market category (e.g., "spot" or "linear").
// It handles pagination to ensure all symbols are fetched.
// It sends the resulting symbol list through the provided channel.
func fetchSymbols(category string, ch chan<- []SymbolInfo) {
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

		fmt.Println(url)

		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			log.Printf("Error creating request for %s symbols: %v", category, err)
			ch <- nil
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

		// Extract the symbol information from the instrument list
		for _, instrument := range apiResponse.Result.List {
			if instrument.Status == "Trading" {
				// For spot market, displayName might be empty, use symbol instead
				displayName := instrument.DisplayName
				if displayName == "" {
					displayName = instrument.Symbol
				}

				allSymbols = append(allSymbols, SymbolInfo{
					Symbol:      instrument.Symbol,
					DisplayName: displayName,
					SettleCoin:  instrument.SettleCoin,
					BaseCoin:    instrument.BaseCoin,
				})
			}
		}

		if apiResponse.Result.NextPageCursor == "" {
			break
		}
		cursor = apiResponse.Result.NextPageCursor
	}

	fmt.Printf("Successfully fetched %d total symbols from the %s market.\n", len(allSymbols), category)
	ch <- allSymbols
}

// mergeAndFilter finds the common symbols between spot and futures markets.
// It uses displayName for matching and settleCoin for categorization.
func mergeAndFilter(spotSymbols, futuresSymbols []SymbolInfo) MatchResult {
	// Create a map of spot symbols using displayName as the key
	spotMap := make(map[string]SymbolInfo)
	for _, symbol := range spotSymbols {
		spotMap[symbol.DisplayName] = symbol
	}

	var allCommon []SymbolInfo
	var usdtQuoted []SymbolInfo
	var usdcQuoted []SymbolInfo
	var usdcPerpMatch []string

	// Track displayNames we've already matched to avoid duplicates
	matched := make(map[string]bool)

	for _, futuresSymbol := range futuresSymbols {
		// Check if this futures symbol's displayName exists in spot
		if spotSymbol, exists := spotMap[futuresSymbol.DisplayName]; exists && !matched[futuresSymbol.DisplayName] {
			matched[futuresSymbol.DisplayName] = true

			// Create combined info (prefer spot symbol name for display)
			info := SymbolInfo{
				Symbol:      spotSymbol.Symbol,
				DisplayName: futuresSymbol.DisplayName,
				SettleCoin:  futuresSymbol.SettleCoin,
				BaseCoin:    futuresSymbol.BaseCoin,
			}

			allCommon = append(allCommon, info)

			// Categorize by settlement coin
			if futuresSymbol.SettleCoin == "USDT" {
				usdtQuoted = append(usdtQuoted, info)
			} else if futuresSymbol.SettleCoin == "USDC" {
				usdcQuoted = append(usdcQuoted, info)
				// Track if this was a PERP suffix match
				if futuresSymbol.Symbol != spotSymbol.Symbol {
					usdcPerpMatch = append(usdcPerpMatch, futuresSymbol.DisplayName)
				}
			}
		}
	}

	// Sort all slices by DisplayName
	sortByDisplayName := func(slice []SymbolInfo) {
		sort.Slice(slice, func(i, j int) bool {
			return slice[i].DisplayName < slice[j].DisplayName
		})
	}

	sortByDisplayName(allCommon)
	sortByDisplayName(usdtQuoted)
	sortByDisplayName(usdcQuoted)
	sort.Strings(usdcPerpMatch)

	return MatchResult{
		AllCommon:     allCommon,
		USDTQuoted:    usdtQuoted,
		USDCQuoted:    usdcQuoted,
		USDCPerpMatch: usdcPerpMatch,
	}
}

// printSymbolTable prints symbols in a formatted table.
func printSymbolTable(symbols []SymbolInfo, columns int) {
	for i, info := range symbols {
		// Format: SYMBOL (SETTLECOIN)
		display := fmt.Sprintf("%s (%s)", info.DisplayName, info.SettleCoin)
		fmt.Printf("%-20s", display)
		if (i+1)%columns == 0 {
			fmt.Println()
		}
	}
	if len(symbols)%columns != 0 {
		fmt.Println()
	}
}

func main() {
	fmt.Println("Fetching tradable symbols from Bybit...")

	spotSymbolsChan := make(chan []SymbolInfo)
	futuresSymbolsChan := make(chan []SymbolInfo)

	go fetchSymbols("spot", spotSymbolsChan)
	go fetchSymbols("linear", futuresSymbolsChan)

	spotSymbols := <-spotSymbolsChan
	futuresSymbols := <-futuresSymbolsChan

	fmt.Println("--------------------------------------------------")

	if spotSymbols == nil || futuresSymbols == nil {
		log.Fatal("Failed to fetch symbols from one or more markets. Exiting.")
		return
	}

	result := mergeAndFilter(spotSymbols, futuresSymbols)

	// Print all common symbols
	fmt.Printf("\n═══════════════════════════════════════════════════\n")
	fmt.Printf("Found %d symbols in BOTH Spot and Futures markets:\n", len(result.AllCommon))
	fmt.Printf("═══════════════════════════════════════════════════\n\n")
	printSymbolTable(result.AllCommon, 4)

	// Print USDT-quoted pairs
	fmt.Printf("\n--------------------------------------------------\n")
	fmt.Printf("USDT-Quoted Pairs: %d\n", len(result.USDTQuoted))
	fmt.Printf("--------------------------------------------------\n\n")
	printSymbolTable(result.USDTQuoted, 4)

	// Print USDC-quoted pairs
	fmt.Printf("\n--------------------------------------------------\n")
	fmt.Printf("USDC-Quoted Pairs: %d\n", len(result.USDCQuoted))
	fmt.Printf("--------------------------------------------------\n\n")
	printSymbolTable(result.USDCQuoted, 4)

	// Show which USDC pairs were matched via PERP suffix
	if len(result.USDCPerpMatch) > 0 {
		fmt.Printf("\n--------------------------------------------------\n")
		fmt.Printf("Note: %d USDC pairs matched via PERP suffix in futures\n", len(result.USDCPerpMatch))
		fmt.Printf("--------------------------------------------------\n")
	}
}
