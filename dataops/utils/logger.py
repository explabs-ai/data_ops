class Logger:
    def __init__(self, op_type=''):
        self.op_type = op_type

    def log(self, op, message):
        print('Exception Occured While {} On The {} : {}'.format(self.op_type, op, message))