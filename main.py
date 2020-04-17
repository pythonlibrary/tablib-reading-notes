import sys
# pythonlibrary.net: 
# 通过源代码的方式直接加载模块
# 因为我们在阅读代码的时候需要调试，因此最好不通过安装而直接加载源代码
sys.path.append("./src")

import tablib

print(tablib.__version__)

data = tablib.Dataset(title='first dataset')


names = ['Kenneth Reitz', 'Bessie Monke']

for name in names:
    # split name appropriately
    fname, lname = name.split()

    # add names to Dataset
    data.append([fname, lname])

print(data)

data.headers = ['First Name', 'Last Name']

print(data)

data.append_col([22, 20], header='Age')


print(data)
print(data.transpose())

print(data.dict)
print(data.export('csv'))

# pythonlibrary.net: two ways to export json format:
#      the first one uses the descriptor of the dataset
#      the second one uses the export method of the dataset
print(data.json)
print(data.export('json'))



print(data[0])
print(data[0:2])

import random

def random_grade(row):
    """Returns a random integer for entry."""
    return (random.randint(60,100)/100.0)

data.append_col(random_grade, header='Grade')

print(data)

print(data)

daniel_tests = [
    ('11/24/09', 'Math 101 Mid-term Exam', 56.),
    ('05/24/10', 'Math 101 Final Exam', 62.)
]

suzie_tests = [
    ('11/24/09', 'Math 101 Mid-term Exam', 56.),
    ('05/24/10', 'Math 101 Final Exam', 62.)
]

# Create new dataset
tests = tablib.Dataset()
tests.headers = ['Date', 'Test Name', 'Grade']

# Daniel's Tests
tests.append_separator('Daniel\'s Scores')

for test_row in daniel_tests:
   tests.append(test_row)

# Susie's Tests
tests.append_separator('Susie\'s Scores')

for test_row in suzie_tests:
   tests.append(test_row)

# Write spreadsheet to disk
with open('grades.xls', 'wb') as f:
    f.write(tests.export('xls'))


# pythonlibrary.net
# 下边是一个描述器的简单小例子

class DescriptorTest():
    def __init__(self):
        self.key = 123

    def __get__(self, obj, type=None) -> object:
        print("accessing the attribute to get the value")
        return self.key
    def __set__(self, obj, value) -> None:
        print("accessing the attribute to set the value")
        self.key=value

class TestClass():
    value = DescriptorTest()

test_obj = TestClass()
x = test_obj.value
print(x)
test_obj.value=18
x = test_obj.value
print(x)