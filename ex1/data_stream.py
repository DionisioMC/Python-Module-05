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

    def ingest(self, data: typing.Union[int, float, list[typing.Union[int, float]]]) -> None:
        if isinstance(data, (int, float)):
            self.data.append((len(self.data), str(data)))
        elif all(isinstance(entry, (int, float)) for entry in data):
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
            self.data.append((len(self.data), str(data)))
        elif all(isinstance(entry, str) for entry in data):
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

    def ingest(self, data: typing.Union[str, list[str]]) -> None:
        if (isinstance(data, dict) and
            (all(isinstance(key, str) for key in data.keys()) and
                    all(isinstance(value, str) for value in data.values()))):
            self.data.append((len(self.data), f"{data['log_level']}: {data['log_message']}"))
        elif all(
                isinstance(entry, dict) and
                all(isinstance(key, str) and
                    isinstance(value, str) 
                    for key, value in entry.items())
                for entry in data):
            for entry in data:
                self.data.append((len(self.data), f"{entry['log_level']}: {entry['log_message']}"))
        else:
            raise Exception("Improper log data")


class DataStream:
    def __init__(self):
        self._processors: list[DataProcessor] = []

    def register_processor(self, proc: DataProcessor) -> None:
        self._processors.append(proc)

    def print_processors_stats(self) -> None:
        print("== DataStream statistics ==")
        if len(self._processors) == 0:
            print("No processor found, no data\n")

    def process_stream(self, stream: list[typing.Any]) -> None:
        for item in stream:
            processed = False
            for processor in self._processors:
                if processor.validate(item):
                    processor.ingest(item)
                    processed = True
            if processed == False:
                print(f"DataStream error - Can't process element in stream: {item}")
        self.print_processors_stats()


if __name__ == "__main__":
    print("=== Code Nexus - Data Stream ===\n")
    print("Initialize Data Stream...")
    data_stream = DataStream()
    data_stream.print_processors_stats()
    print("Registering Numeric Processor\n")
    data_stream.register_processor(NumericProcessor())
    test = ['Hello world', [3.14, -1, 2.71],
            [{'log_level': 'WARNING', 'log_message': 'Telnet access! Use ssh instead'}, 
             {'log_level': 'INFO', 'log_message': 'User wil isconnected'}], 42, ['Hi', 'five']]
    print(f"Send first batch of data on stream: {test}")
    data_stream.process_stream(test)