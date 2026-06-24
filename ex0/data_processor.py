import abc
import typing


class DataProcessor(abc.ABC):
    def __init__(self) -> None:
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
    def __init__(self) -> None:
        super().__init__()

    def validate(self, data: typing.Any) -> bool:
        if isinstance(data, (int, float)):
            return True
        elif isinstance(data, list):
            return all(isinstance(entry, (int, float)) for entry in data)
        else:
            return False

    def ingest(self, data:
               typing.Union[int, float,
                            list[typing.Union[int, float]]]) -> None:
        if isinstance(data, (int, float)):
            self.data.append((len(self.data), str(data)))
        elif (isinstance(data, list) and all(
              isinstance(entry, (int, float)) for entry in data)):
            for entry in data:
                self.data.append((len(self.data), str(entry)))
        else:
            raise Exception("Improper numeric data")


class TextProcessor(DataProcessor):
    def __init__(self) -> None:
        super().__init__()

    def validate(self, data: typing.Any) -> bool:
        if isinstance(data, str):
            return True
        elif isinstance(data, list):
            return all(isinstance(entry, str) for entry in data)
        else:
            return False

    def ingest(self, data: typing.Union[str, list[str]]) -> None:
        if isinstance(data, str):
            self.data.append((len(self.data), data))
        elif (isinstance(data, list) and all(
              isinstance(entry, str) for entry in data)):
            for entry in data:
                self.data.append((len(self.data), str(entry)))
        else:
            raise Exception("Improper text data")


class LogProcessor(DataProcessor):
    def __init__(self) -> None:
        super().__init__()

    def validate(self, data: typing.Any) -> bool:
        if isinstance(data, dict):
            return (all(isinstance(key, str) for key in data.keys()) and
                    all(isinstance(value, str) for value in data.values()))
        elif isinstance(data, list):
            return all(
                isinstance(entry, dict) and
                all(isinstance(key, str) and
                    isinstance(value, str)
                    for key, value in entry.items())
                for entry in data)
        else:
            return False

    def ingest(self, data:
               typing.Union[dict[str, str], list[dict[str, str]]]) -> None:
        if (isinstance(data, dict) and
            (all(isinstance(key, str) for key in data.keys()) and
                all(isinstance(value, str) for value in data.values()))):
            self.data.append((len(self.data), f"{data['log_level']}: "
                              f"{data['log_message']}"))
        elif (isinstance(data, list) and all(
                isinstance(entry, dict) and
                all(isinstance(key, str) and
                    isinstance(value, str)
                    for key, value in entry.items())
                for entry in data)):
            entry: dict[str, str]
            for entry in data:
                self.data.append((len(self.data), f"{entry['log_level']}: "
                                  f"{entry['log_message']}"))
        else:
            raise Exception("Improper log data")


if __name__ == "__main__":
    print("=== Code Nexus - Data Processor ===\n")
    print("Testing Numeric Processor...")
    num_processor = NumericProcessor()
    print(f"Trying to validate input '42': {num_processor.validate(42)}")
    print(
        f"Trying to validate input 'Hello': {num_processor.validate('Hello')}")
    print("Test invalid ingestion of string 'foo' without prior validation:")
    try:
        num_processor.ingest("foo")  # type: ignore
    except Exception as e:
        print(f"Got exception: {e}")
    num_test: list[int | float] = [1, 2, 3, 4, 5]
    print(f"Processing data: {num_test}")
    if num_processor.validate(num_test):
        num_processor.ingest(num_test)
    print("Extracting 3 values...")
    for i in range(3):
        num, value = num_processor.output()
        print(f"Numeric value {num}: {value}")
    print("\nTesting Text Processor...")
    text_processor = TextProcessor()
    print(f"Trying to validate input '42': {text_processor.validate(42)}")
    text_test = ['Hello', 'Nexus', 'World']
    print(f"Processing data: {text_test}")
    if text_processor.validate(text_test):
        text_processor.ingest(text_test)
    print("Extracting 1 value...")
    num, value = text_processor.output()
    print(f"Text value {num}: {value}")
    print("\nTesting Log Processor...")
    log_processor = LogProcessor()
    print("Trying to validate input 'Hello': "
          f"{log_processor.validate('Hello')}")
    log_test = [{'log_level': 'NOTICE', 'log_message': 'Connection to server'},
                {'log_level': 'ERROR', 'log_message': 'Unauthorized access!!'}]
    print(f"Processing data: {log_test}")
    if log_processor.validate(log_test):
        log_processor.ingest(log_test)
    print("Extracting 2 values...")
    for i in range(2):
        num, value = log_processor.output()
        print(f"Log entry {num}: {value}")
