import json
import typing as t

if t.TYPE_CHECKING:
    from pathlib import Path

    from singerlake.stream import Stream


class TestStreamWriter:
    def __init__(self, input_stream_path: "Path"):
        self.input_stream_path = input_stream_path
        self.stream_schema: dict | None = None

    def write_messages_to_stream(self, stream: "Stream"):
        """Write messages from a file to a stream."""

        with stream.record_writer() as writer:
            with open(self.input_stream_path, "r", encoding="utf-8") as input_stream:
                for line in input_stream:
                    message = json.loads(line)

                    if message["type"] == "SCHEMA":
                        self.stream_schema = message

                    if message["type"] == "RECORD":
                        writer.write(schema=self.stream_schema, record=message)

        return stream
