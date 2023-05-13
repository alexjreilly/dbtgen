import logging


class CustomLogger(logging.getLoggerClass()):

    def __init__(self):
        super().__init__('logger')

        self.format = {
            'fmt': '%(asctime)s | %(message)s',
            'datefmt': '%H:%M:%S',
        }
        formatter = logging.Formatter(**self.format)

        self.setLevel(logging.INFO)
        self.handlers = []

        handler = logging.StreamHandler()
        handler.setFormatter(formatter)
        self.addHandler(handler)

    def status(
            self,
            message: str,
            status: str
    ) -> None:
        """
        Logs a message to the terminal with a status. Formats message with 
        trailing '...'

        :param message: The text to be formatted and displayed to the terminal
        :param status: A flag to indicate the status
        """
        colours = {
            'RUN': '',
            'DONE': '\033[92m',
            'CREATED': '\033[92m',
            'SKIPPED' : '\033[93m',
            'FAILED': '\033[91m'
        }
        reset = '\x1b[0m'

        self.info(
            f"{message} {(80 - len(message)) * '.'} "
            f"[{colours.get(status) + status + reset}]"
        )
