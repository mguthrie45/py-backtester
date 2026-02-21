def handler(stock_slices, capital_slices, holdings_slices) -> str | None:
    return ["buy", "AAPL", 1] if stock_slices.iloc[-1].close > 238 else None
