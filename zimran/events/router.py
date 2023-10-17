from zimran.events.dto import EventHandler, Exchange


class Router:
    def __init__(self, event_handlers: dict[str, EventHandler] | None = None):
        self.__event_handlers = event_handlers or {}

        for name, handler in self.__event_handlers.items():
            if not isinstance(name, str):
                raise TypeError('Event name must be str')

            if not isinstance(handler, EventHandler):
                raise TypeError('Event handler must be instance of EventHandler')

    def handle_event(
        self,
        name: str,
        *,
        exchange: Exchange | None = None,
        requeue: bool = False,
        reject_on_redelivered: bool = False,
        ignore_processed: bool = False,
    ):
        def wrapper(func):
            self.__event_handlers[name] = EventHandler(
                exchange=exchange,
                handler=func,
                requeue=requeue,
                reject_on_redelivered=reject_on_redelivered,
                ignore_processed=ignore_processed,
            )

        return wrapper

    def add_event_handler(
        self,
        name: str,
        handler: callable,
        *,
        exchange: Exchange | None = None,
        requeue: bool = False,
        reject_on_redelivered: bool = False,
        ignore_processed: bool = False,
    ):
        self.__event_handlers[name] = EventHandler(
            exchange=exchange,
            handler=handler,
            requeue=requeue,
            reject_on_redelivered=reject_on_redelivered,
            ignore_processed=ignore_processed,
        )

    @property
    def handlers(self) -> dict[str, EventHandler]:
        return self.__event_handlers
