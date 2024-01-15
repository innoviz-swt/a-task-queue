# a-task-queue
An in process task queue for distributed computing systems.

## Usage
```python
from ataskq import TaskQ, Task, targs

# create  job
tr = TaskQ().create_job()

# add tasks
# entrypoint stands for the relevant function import statement
# (here we use build in demo functions)
tr.add_tasks([
    Task(entrypoint='ataskq.tasks_utils.hello_world'),
    Task(entrypoint='ataskq.tasks_utils.dummy_args_task', targs=targs(
        'arg0', 'arg1', kwarg1=10, kwarg2='this is kwarg2')),
])

# run the tasks
tr.run() # to run in parallel add num_processes=N
```

more example can be found [here](./examples)

## Contributer
setup project git hooks
```
contrib/setup.sh
```

### vs code
to get nominal vscode settings run
```
./contrib/.vscode/init.sh
```
