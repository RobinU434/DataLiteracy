import sys
import time

import threading


class SpinnerThread(threading.Thread):

    def __init__(self):
        super().__init__(target=self._spin)
        self._stopevent = threading.Event()

    def stop(self):
        self._stopevent.set()

    def _spin(self):

        while not self._stopevent.isSet():
            for t in '|/-\\':
                sys.stdout.write(t)
                sys.stdout.flush()
                time.sleep(0.1)
                sys.stdout.write('\b')


def long_task():
    for i in range(10):
        time.sleep(1)
        print('Tick {:d}'.format(i))


def main():

    task = threading.Thread(target=long_task)
    task.start()

    spinner_thread = SpinnerThread()
    spinner_thread.start()

    task.join()
    spinner_thread.stop()


if __name__ == '__main__':
    main()