def cleanup_and_normalize_queue_name(queue_name: str):
    if '*' in queue_name:
        queue_name = queue_name.replace('*', '')

    if '#' in queue_name:
        queue_name = queue_name.replace('#', '')

    if '..' in queue_name:
        queue_name = queue_name.replace('..', '.')

    if queue_name.endswith('.'):
        queue_name = queue_name[:-1]

    if '-' in queue_name:
        queue_name = queue_name.replace('-', '_')

    return f'{queue_name}_q'