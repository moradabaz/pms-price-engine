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

    def values(self):
        return list(self._data.values())


class FakeValueState:
    """In-memory stand-in for PyFlink's ValueState."""

    def __init__(self):
        self._value = None

    def value(self):
        return self._value

    def update(self, value):
        self._value = value


class FakeRuntimeContext:
    """Returns one Fake*State per descriptor name, reused across calls."""

    def __init__(self):
        self._states = {}

    def get_map_state(self, descriptor):
        return self._states.setdefault(descriptor.get_name(), FakeMapState())

    def get_state(self, descriptor):
        return self._states.setdefault(descriptor.get_name(), FakeValueState())


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


class FakeMultiState:
    """Dispatches get_broadcast_state(descriptor) by descriptor name, for
    stages that broadcast() with more than one MapStateDescriptor (Phase 19:
    Stage A0/A4) — the single-shared-state FakeReadOnlyContext/
    FakeBroadcastContext above only work for a stage with exactly one."""

    def __init__(self):
        self._states = {}

    def get_broadcast_state(self, descriptor):
        return self._states.setdefault(descriptor.get_name(), FakeMapState())

    def timer_service(self):
        return FakeTimerService()
