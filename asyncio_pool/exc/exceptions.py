class EventLoopStoppedError(Exception):
    def init(self, expression, message):
        self.expression = expression
        self.message = message


class EventLoopStoppedError(Exception):
    def init(self, expression, message):
        self.expression = expression
        self.message = message