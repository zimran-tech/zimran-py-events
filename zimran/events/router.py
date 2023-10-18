from zimran.events.dto import EventHandler, Exchange


class Router:
    def __init__(self, event_handlers: dict[str, EventHandler] | None = None):
        self.__event_handlers = event_handlers or {}

        for name, handler in self.__event_handlers.items():
            if not isinstance(name, str):
                raise TypeError('Event name must be str')

            if not isinstance(handler, EventHandler):
                raise TypeError('Event handler must be instance of EventHandler')

    def handle_event(self, name: str, *, exchange: Exchange | None = None, **kwargs):
        """
        :param name: Event name(Routing key) for which handler will be called.
        :param exchange: Exchange for event.

        :param kwargs: EventHandler params. See EventHandler class for more info.
        """

        def wrapper(func):
            self.__event_handlers[name] = EventHandler(exchange=exchange, handler=func, **kwargs)

        return wrapper

    def add_event_handler(self, name: str, handler: callable, *, exchange: Exchange | None = None, **kwargs):
        """
        :param name: Event name(Routing key) for which handler will be called.
        :param exchange: Exchange for event.

        :param kwargs: EventHandler params. See EventHandler class for more info.
        """

        self.__event_handlers[name] = EventHandler(exchange=exchange, handler=handler, **kwargs)

    @property
    def handlers(self) -> dict[str, EventHandler]:
        return self.__event_handlers
