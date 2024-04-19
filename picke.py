import pickle

import marshal
import types


def f(a,b):
    return a+b


data = pickle.dumps(marshal.dumps(f.__code__))

# data = pickle.dumps(f)
with open('data.pkl', 'wb') as f:
    f.write(data)
#
#
with open('data.pkl', 'rb') as f:
    data = pickle.loads(f.read())
    code= marshal.loads(data)
    f = types.FunctionType(code, globals(), "some_func_name")
    print(f(1,2))