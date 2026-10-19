"""Mirror console output into the configured runtime log without terminal colors."""
import logging
import re


class LoggedConsole:
    def __init__(self, stream):
        self.stream = stream
        self.pending = ""

    def write(self, text):
        result = self.stream.write(text)
        self.pending += re.sub(r"\x1b\[[0-?]*[ -/]*[@-~]", "", text)
        while "\n" in self.pending:
            line, self.pending = self.pending.split("\n", 1)
            logging.getLogger("hypertrader.console").info(line)
        return result

    def flush(self):
        if self.pending:
            logging.getLogger("hypertrader.console").info(self.pending)
            self.pending = ""
        self.stream.flush()

    def __getattr__(self, name):
        return getattr(self.stream, name)
