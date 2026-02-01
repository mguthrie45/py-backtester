import sys
from controller.main import MainController


if __name__ == "__main__":
    test_name = sys.argv[1]
    controller = MainController(test_name)
    controller.backtest()
