import abc
import typing


class DataProcessor(abc.ABC):
    def __init__(self) -> None:
        self._data: list[tuple[int, str]] = []
        self._processed_items = 0

    @abc.abstractmethod
    def validate(self, data: typing.Any) -> bool:
        pass

    @abc.abstractmethod
    def ingest(self, data: typing.Any) -> None:
        pass

    def print_stats(self) -> None:
        print(f"{self.__class__.__name__}: total {self._processed_items} "
              f"items processed, remaining {len(self._data)} on processor")

    def output(self) -> tuple[int, str]:
        entry = self._data.pop(0)
        return entry


class NumericProcessor(DataProcessor):
    def __init__(self) -> None:
        super().__init__()
        self.__class__.__name__ = "Numeric Processor"

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
            self._data.append((len(self._data), str(data)))
            self._processed_items += 1
        elif (isinstance(data, list) and all(
              isinstance(entry, (int, float)) for entry in data)):
            for entry in data:
                self._data.append((len(self._data), str(entry)))
                self._processed_items += 1
        else:
            raise Exception("Improper numeric data")


class TextProcessor(DataProcessor):
    def __init__(self) -> None:
        super().__init__()
        self.__class__.__name__ = "Text Processor"

    def validate(self, data: typing.Any) -> bool:
        if isinstance(data, str):
            return True
        elif isinstance(data, list):
            return all(isinstance(entry, str) for entry in data)
        else:
            return False

    def ingest(self, data: typing.Union[str, list[str]]) -> None:
        if isinstance(data, str):
            self._data.append((len(self._data), data))
            self._processed_items += 1
        elif (isinstance(data, list) and all(
              isinstance(entry, str) for entry in data)):
            for entry in data:
                self._data.append((len(self._data), str(entry)))
                self._processed_items += 1
        else:
            raise Exception("Improper text data")


class LogProcessor(DataProcessor):
    def __init__(self) -> None:
        super().__init__()
        self.__class__.__name__ = "Log Processor"

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
            self._data.append(
                (len(self._data), f"{data['log_level']}: "
                 f"{data['log_message']}"))
            self._processed_items += 1
        elif (isinstance(data, list) and all(
                isinstance(entry, dict) and
                all(isinstance(key, str) and
                    isinstance(value, str)
                    for key, value in entry.items())
                for entry in data)):
            for entry in data:
                self._data.append(
                    (len(self._data), f"{entry['log_level']}: "
                     f"{entry['log_message']}"))
                self._processed_items += 1
        else:
            raise Exception("Improper log data")


class DataStream:
    def __init__(self) -> None:
        self._processors: list[DataProcessor] = []

    def get_processors(self) -> list[DataProcessor]:
        return self._processors

    def register_processor(self, proc: DataProcessor) -> None:
        self._processors.append(proc)

    def print_processors_stats(self) -> None:
        print("== DataStream statistics ==")
        if len(self._processors) == 0:
            print("No processor found, no data\n")
        else:
            for processor in self._processors:
                processor.print_stats()
            print()

    def process_stream(self, stream: list[typing.Any]) -> None:
        for item in stream:
            processed = False
            for processor in self._processors:
                if processor.validate(item):
                    processor.ingest(item)
                    processed = True
            if processed is False:
                print(
                    "DataStream error - Can't process element in stream: "
                    f"{item}")
        self.print_processors_stats()


if __name__ == "__main__":
    print("=== Code Nexus - Data Stream ===\n")
    print("Initialize Data Stream...")
    data_stream = DataStream()
    data_stream.print_processors_stats()
    print("Registering Numeric Processor\n")
    data_stream.register_processor(NumericProcessor())
    test = ['Hello world', [3.14, -1, 2.71],
            [{'log_level': 'WARNING',
              'log_message': 'Telnet access! Use ssh instead'},
             {'log_level': 'INFO', 'log_message': 'User wil isconnected'}],
            42, ['Hi', 'five']]
    print(f"Send first batch of data on stream: {test}")
    data_stream.process_stream(test)
    print("Registering other data processors")
    data_stream.register_processor(TextProcessor())
    data_stream.register_processor(LogProcessor())
    print("Send the same batch again")
    data_stream.process_stream(test)
    print("Consume some elements from the data processors: "
          "Numeric 3, Text 2, Log 1")
    num_processor, text_processor, log_processor = data_stream.get_processors()
    for i in range(3):
        num_processor.output()
    for i in range(2):
        text_processor.output()
    log_processor.output()
    data_stream.print_processors_stats()
