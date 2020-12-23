"""# submit
for some_str in [ "foo", "bar" ]:
    submit(my_str=some_str)
"""# endsubmit

import qondor

print("This is {0}".format(qondor.scope.my_str))