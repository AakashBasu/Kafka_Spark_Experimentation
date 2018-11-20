# import time
# import csv
# import random
#
# def id_generator(size=6, chars=string.ascii_uppercase + string.digits):
#     return ''.join(random.choice(chars) for _ in range(size))
#
# while True:
#     print('I\'m working...')
#     with open('mycsvfile.csv', 'ab') as f:
#         writer = csv.writer(f)
#         writer.writerow([])
#         writer.writerow(['0', '0', '0'])
#     time.sleep(2)
#
#
