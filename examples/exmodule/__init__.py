import time

def hello_world():
    print('hello world')

def entrypoint_with_args(*args, **kwargs):
    if 'sleep' in kwargs:
        time.sleep(kwargs['sleep'])
    print(f'entrypoint_with_args args: {args}, kwargs: {kwargs}')
