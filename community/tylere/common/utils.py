def a():
    print('root level function a')

class c:  
    def __init__(self):
        print('class c init')

    def a():
        print('class c function a')

    def b():
        print('class c function b')

    class d:
        def e():
            print('class c class d function e')
