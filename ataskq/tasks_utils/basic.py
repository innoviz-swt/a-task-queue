import time


def hello_world():
    print("hello world")


def dummy_args_task(*args, **kwargs):
    if "sleep" in kwargs:
        time.sleep(kwargs["sleep"])
    print(f"entrypoint_with_args args: {args}, kwargs: {kwargs}")


def exception_task(etype=Exception, message="This is an exception task"):
    raise etype(message)
