import re

with open('Atomix.py', 'r') as f:
    code = f.read()

# 1. Version
code = code.replace('Version: 3.1.6', 'Version: 3.2.0')
code = code.replace('Atomix v3.1', 'Atomix v3.2')
code = code.replace('Atomix v3.0', 'Atomix v3.2')

# 2. Ref.set()
code = code.replace(
    'return dosync(lambda: set(self, value))()',
    '''result = [None]
            @atomically
            def _do_set():
                result[0] = write(self, value)
            _do_set()
            return result[0]'''
)

# 3. Ref.alter()
code = code.replace(
    'return dosync(lambda: alter(self, fn, *args, **kwargs))()',
    '''result = [None]
            @atomically
            def _do_alter():
                result[0] = alter(self, fn, *args, **kwargs)
            _do_alter()
            return result[0]'''
)

# 4. Ref.commute()
code = code.replace(
    'return dosync(lambda: commute(self, fn, *args, **kwargs))()',
    '''result = [None]
            @atomically
            def _do_commute():
                result[0] = commute(self, fn, *args, **kwargs)
            _do_commute()
            return result[0]'''
)

# 5. transaction decorator rename
code = code.replace(
    'def transaction(timeout: float = 10.0, max_retries: int = 500):',
    'def transactional(timeout: float = 10.0, max_retries: int = 500):'
)

with open('Atomix.py', 'w') as f:
    f.write(code)
print("Basic fixes applied")
