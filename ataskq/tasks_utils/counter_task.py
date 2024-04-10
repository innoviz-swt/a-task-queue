class CounterKWArg:
    def __init__(self, val=0, name="counter") -> None:
        self._val = val
        self._name = name

    @property
    def val(self):
        return self._val

    @property
    def name(self):
        return self._name

    def count(self):
        ret = self._val
        self._val += 1

        return ret


def counter_kwarg():
    return CounterKWArg()


def counter_task(counter: CounterKWArg = None, print_counter=False):
    assert counter is not None, "counter value must be provided"

    if print_counter:
        print(f"counter name: {counter.name}, val: {counter.val}")
    counter.count()
