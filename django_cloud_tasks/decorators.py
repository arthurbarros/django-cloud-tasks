from functools import wraps, partial

from .base import BaseTask, CloudTaskWrapper
from .registries import registry


def _gen_internal_task_name(task_func):
    internal_task_name = '.'.join((task_func.__module__, task_func.__name__))
    return internal_task_name


def create_task(task_class, func, **kwargs):
    run = partial(func)

    internal_task_name = _gen_internal_task_name(func)
    attrs = {
        'internal_task_name': internal_task_name,
        'run': run,
        '__module__': func.__module__,
        '__doc__': func.__doc__}
    attrs.update(kwargs)

    return type(func.__name__, (task_class,), attrs)()


def task(queue=None, task_handler_url=None, delay_seconds=0, **headers):
    def decorator(func):
        task_cls = create_task(BaseTask, func)
        registry.register(task_cls)

        @wraps(func)
        def inner_run(**kwargs):
            """
            Executa a tarefa em uma fila específica com um atraso opcional.

            Parâmetros:
            - runtime_queue (str, opcional): O nome da fila em que a tarefa será enfileirada. 
                                             Se não fornecido, usará o valor de 'queue' definido no decorador.
            - delay_seconds (int, opcional): O número de segundos para atrasar a execução da tarefa.
                                             O valor padrão é 0.

            Outros parâmetros em **kwargs serão passados para a tarefa.
            """
            actual_queue = kwargs.pop('queue', queue)
            actual_delay_seconds = kwargs.pop('delay_seconds', delay_seconds)
            return CloudTaskWrapper(
                task_cls, actual_queue, kwargs, task_handler_url=task_handler_url, delay_seconds=actual_delay_seconds, headers=headers)

        return inner_run

    return decorator
