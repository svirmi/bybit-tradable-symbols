# bybit-tradable-symbols

Service to track current tradable ByBit symbols for spot and futures

```
http://localhost:8080/intersection/futures
```

##### Can be splitted into:

main.go // Server setup, routes, main()
cache.go // SymbolCache + update logic
handlers.go // HTTP handlers
bybit.go // Bybit API client
types.go // Structs (SymbolInfo, etc.)
