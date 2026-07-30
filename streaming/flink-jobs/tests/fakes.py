class FakeMapState:
    """In-memory stand-in for PyFlink's MapState."""

    def __init__(self):
        self._data = {}

    def get(self, key):
        return self._data.get(key)

    def put(self, key, value):
        self._data[key] = value

    def remove(self, key):
        self._data.pop(key, None)

    def items(self):
        return list(self._data.items())


class FakeRuntimeContext:
    """Returns one FakeMapState per descriptor name, reused across calls."""

    def __init__(self):
        self._states = {}

    def get_map_state(self, descriptor):
        return self._states.setdefault(descriptor.get_name(), FakeMapState())


class FakeTimerService:
    """Records registered processing-time timers."""

    def __init__(self):
        self.registered = []

    def register_processing_time_timer(self, timestamp):
        self.registered.append(timestamp)


class FakeReadOnlyContext:
    """Fake ReadOnlyContext for process_element, backed by shared broadcast state."""

    def __init__(self, broadcast_state):
        self._broadcast_state = broadcast_state
        self.timer_service_ = FakeTimerService()

    def get_broadcast_state(self, descriptor):
        return self._broadcast_state

    def timer_service(self):
        return self.timer_service_


class FakeBroadcastContext:
    """Fake Context for process_broadcast_element."""

    def __init__(self, broadcast_state):
        self._broadcast_state = broadcast_state

    def get_broadcast_state(self, descriptor):
        return self._broadcast_state
