import abc
import typing


class DataProcessor(abc.ABC):
    def __init__(self):
        self.data: list[tuple[int, str]] = []

    @abc.abstractmethod
    def validate(self, data: typing.Any) -> bool:
        pass

    @abc.abstractmethod
    def ingest(self, data: typing.Any) -> None:
        pass

    def output(self) -> tuple[int, str]:
        entry = self.data.pop(0)
        return entry


class NumericProcessor(DataProcessor):
    def __init__(self):
        super().__init__()

    def validate(self, data: typing.Union[int, float, list[typing.Union[int, float]]]) -> bool:
        if isinstance(data, (int, float)) or all(isinstance(entry, (int, float)) for entry in data):
            return True
        else:
            return False

    def ingest(self, data: typing.Union[int, float, list[typing.Union[int, float]]]) -> None:
        if isinstance(data, (int, float)):
            self.data.append((len(self.data), str(data)))
        elif all(isinstance(entry, (int, float)) for entry in data):
            for entry in data:
                self.data.append((len(self.data), str(entry)))
        else:
            raise Exception("Improper numeric data")


class TextProcessor(DataProcessor):
    def __init__(self):
        super().__init__()

    def validate(self, data: typing.Union[str, list[str]]) -> bool:
        if isinstance(data, str) or all(isinstance(entry, str) for entry in data):
            return True
        else:
            return False

    def ingest(self, data: typing.Union[str, list[str]]) -> None:
        if isinstance(data, str):
            self.data.append((len(self.data), str(data)))
        elif all(isinstance(entry, str) for entry in data):
            for entry in data:
                self.data.append((len(self.data), str(entry)))
        else:
            raise Exception("Improper numeric data")


if __name__ == "__main__":
    print("=== Code Nexus - Data Processor ===\n")
    print("Testing Numeric Processor...")
    num_processor = NumericProcessor()
    print(f"Trying to validate input '42': {num_processor.validate(42)}")
    print(
        f"Trying to validate input 'Hello': {num_processor.validate('Hello')}")
    print("Test invalid ingestion of string 'foo' without prior validation:")
    try:
        num_processor.ingest("foo")
    except Exception as e:
        print(f"Got exception: {e}")
    num_test = [1, 2, 3, 4, 5]
    print(f"Processing data: {num_test}")
    num_processor.ingest(num_test)
    print("Extracting 3 values...")
    for i in range(3):
        num, value = num_processor.output()
        print(f"Numeric value {num}: {value}")
    print("\nTesting Text Processor...")
    text_processor = TextProcessor()
    print(f"Trying to validate input '42': {text_processor.validate(42)}")
