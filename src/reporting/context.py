import random
import string


class ReportingContext:
    report_id: str

    def __init__(self, test_name: str, ticker: str):
        self.__dedup_id = "".join(
            [random.choice(string.ascii_lowercase + string.digits) for _ in range(4)]
        )
        self.report_id = f"{test_name}_{ticker}_{self.__dedup_id}"
