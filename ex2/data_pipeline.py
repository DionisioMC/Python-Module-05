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
        print(f"{self.__class__.__name__}: total {self._processed_items} items processed, remaining {len(self._data)} on processor")

    def output(self) -> tuple[int, str]:
        entry = self._data.pop(0)
        return entry
    
    def get_data(self):
        return self._data


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

    def ingest(self, data: typing.Union[int, float, list[typing.Union[int, float]]]) -> None:
        if isinstance(data, (int, float)):
            self._data.append((len(self._data), str(data)))
            self._processed_items += 1
        elif all(isinstance(entry, (int, float)) for entry in data):
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
        elif all(isinstance(entry, str) for entry in data):
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

    def ingest(self, data: typing.Union[str, list[str]]) -> None:
        if (isinstance(data, dict) and
            (all(isinstance(key, str) for key in data.keys()) and
                    all(isinstance(value, str) for value in data.values()))):
            self._data.append((len(self._data), f"{data['log_level']}: {data['log_message']}"))
            self._processed_items += 1
        elif all(
                isinstance(entry, dict) and
                all(isinstance(key, str) and
                    isinstance(value, str) 
                    for key, value in entry.items())
                for entry in data):
            for entry in data:
                self._data.append((len(self._data), f"{entry['log_level']}: {entry['log_message']}"))
                self._processed_items += 1
        else:
            raise Exception("Improper log data")


class ExportPlugin(typing.Protocol):
    def process_output(self, data: list[tuple[int, str]]) -> None:
        pass
    
class CSVPlugin:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        proc_data = []
        for entry in data:
            proc_data.append(entry[1])
        print("CSV Output:")
        print(",".join(proc_data))

class JSONPlugin:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        proc_data = dict()
        for entry in data:
            proc_data[f"item_{entry[0]}"] = entry[1]
        print("JSON Output:")
        print(proc_data)


class DataStream:
    def __init__(self):
        self._processors: list[DataProcessor] = []
        
    def get_processors(self) -> list[DataProcessor]:
        return self._processors

    def register_processor(self, proc: DataProcessor) -> None:
        self._processors.append(proc)

    def print_processors_stats(self) -> None:
        print("\n== DataStream statistics ==")
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
            if processed == False:
                print(f"DataStream error - Can't process element in stream: {item}")
        self.print_processors_stats()

    def output_pipeline(self, nb: int, plugin: ExportPlugin) -> None:
        for processor in self._processors:
            output_list = []
            for i in range(nb):
                if len(processor.get_data()) > 0:
                    output_list.append(processor.output())
            plugin.process_output(output_list)
        self.print_processors_stats()
            



if __name__ == "__main__":
    print("=== Code Nexus - Data Pipeline ===\n")
    print("Initialize Data Stream...\n")
    data_stream = DataStream()
    data_stream.print_processors_stats()
    print("Registering Processors\n")
    data_stream.register_processor(NumericProcessor())
    data_stream.register_processor(TextProcessor())
    data_stream.register_processor(LogProcessor())
    test = ['Hello world', [3.14, -1, 2.71],
            [{'log_level': 'WARNING', 'log_message': 'Telnet access! Use ssh instead'}, 
             {'log_level': 'INFO', 'log_message': 'User wil isconnected'}], 42, ['Hi', 'five']]
    print(f"Send first batch of data on stream: {test}")
    data_stream.process_stream(test)
    csv = CSVPlugin()
    print("Send 3 processed data from each processor to a CSV plugin:")
    data_stream.output_pipeline(3, csv)
    json = JSONPlugin()
    another_test = [21, 
                    ['I love AI', 'LLMs are wonderful', 'Stay healthy'],
                    [{'log_level': 'ERROR', 'log_message': '500 server crash'},
                     {'log_level': 'NOTICE', 'log_message': 'Certificate expires in 10 days'}],
                    [32, 42, 64, 84, 128, 168], 'World hello']
    print(f"Send another batch of data: {another_test}")
    data_stream.process_stream(another_test)
    data_stream.output_pipeline(5, json)
