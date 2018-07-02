import time
import sys

def print_progress(count, start_time, total):
    """Print a progress bar to the console.
    Credit: https://gist.github.com/vladignatyev/06860ec2040cb497f0f3
    """

    bar_len = 60
    filled_len = int(round(bar_len * count / float(total)))

    percents = round(100.0 * count / float(total), 1)
    bar = '=' * filled_len + '-' * (bar_len - filled_len)

    duration = time.time() - start_time
    completion = '~ 0:0:0 remaining'
    if count > 0:
        avg_time = duration / count
        avg_time = (avg_time * (total - count))
        m, s = divmod(avg_time, 60)
        h, m = divmod(m, 60)
        completion = ('~ %d:%02d:%02d remaining' % (h, m, s))

    sys.stdout.write('[%s] %s%s (%s%s%s) %s\r' %
                     (bar, percents, '%', count, '/', total, completion))
    sys.stdout.flush()
