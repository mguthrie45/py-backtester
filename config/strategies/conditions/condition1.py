# TODO: make this handler's params option based. only ask for what the user specifies
# TODO: supply a list of stock slices that can be changed in config
# TODO: portfolio history? dataframe?
# TODO: Find a way to manage a portfolio data frame state. Keep in mind we'll do this for holdings at some point too.
def handler(slices_with_lookback, portfolio) -> str | None:
    return "buy" if slices_with_lookback[-1].close > 238 else None
