def udf():
    print('test')

    from utils import a

    a()

    from utils import c

    c()
    
    c.d.e()
