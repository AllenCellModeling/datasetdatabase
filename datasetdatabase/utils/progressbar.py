#!/usr/bin/env python

# installed
from typing import Union
import types
import time


class ProgressBar(object):
    """
    Print a progress bar to the console.
    Credit: https://gist.github.com/vladignatyev/06860ec2040cb497f0f3

    Changes to convert from single function to class for better state
    management and easier update.
    """

    def __init__(self, max: int, bar_length: int = 60, increment: int = 1):
        self.current = 0
        self.max = max
        self.start = time.time()
        self.increment_by = increment

        # to calc
        self.bar_length = bar_length
        self.update_bar_str()
        self.remaining_h = 0
        self.remaining_m = 0
        self.remaining_s = 0
        self.update_time_str()


    def increment(self):
        self.current += self.increment_by
        self.draw()


    def draw(self):
        # calc bar and percent
        self.update_bar_str()
        # calc completion
        self.update_time_str()

        # draw
        disp = "\r[{b}] {p}% {c}/{m} {t}".format(b=self.bar,
                                               p=self.percent,
                                               c=self.current,
                                               m=self.max,
                                               t=self.time)
        if self.current != self.max:
            print(disp, end="")
        else:
            print(disp)


    def update_bar_str(self):
        if self.max > 0:
            complete = self.current / float(self.max)
        else:
            complete = 1.0
        self.percent = round(100.0 * complete, 1)
        filled_length = int(round(self.bar_length * complete))

        # bar
        self.bar = "=" * filled_length
        self.bar += "-" * (self.bar_length - filled_length)


    def update_time_str(self):
        if self.current > 0:
            # elapsed time
            duration = time.time() - self.start

            # average time per object
            avg_time = duration / self.current
            avg_time = (avg_time * (self.max - self.current))

            self.remaining_m, self.remaining_s = divmod(avg_time, 60)
            self.remaining_h, self.remaining_m = divmod(self.remaining_m, 60)

            self.remaining_h = round(self.remaining_h)
            self.remaining_m = round(self.remaining_m)
            self.remaining_s = round(self.remaining_s)

        self.time = "~ {h}:{m}:{s} remaining".format(h=self.remaining_h,
                                                     m=self.remaining_m,
                                                     s=self.remaining_s)


    def apply_and_update(self, func: types.ModuleType, **kwargs):
        complete_val = func(**kwargs)
        self.increment()
        return complete_val
